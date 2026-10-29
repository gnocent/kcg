package main

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
)

// tp désigne une partition d'un topic.
type tp struct {
	topic     string
	partition int32
}

func (a tp) less(b tp) bool {
	if a.topic != b.topic {
		return a.topic < b.topic
	}
	return a.partition < b.partition
}

// none marque l'absence d'offset committé.
const none int64 = -1

type group struct {
	name    string
	state   string // état Kafka : Empty, Stable, Assigning…
	matched bool   // passe le filtre --group (sinon nommé par --from, --to ou une sauvegarde)
	offsets map[tp]int64
}

// active reprend la règle de kafka-consumer-groups : un groupe ni Empty ni
// Dead a des membres, et Kafka refuse d'en modifier les offsets.
func (g *group) active() bool {
	return g.state != "Empty" && g.state != "Dead" && g.state != ""
}

func (g *group) offset(p tp) int64 {
	if o, ok := g.offsets[p]; ok {
		return o
	}
	return none
}

// state est la photographie lue sur le cluster pour les topics et les groupes
// filtrés.
type state struct {
	partitions []tp // toutes les partitions des topics filtrés, triées
	start, end map[tp]int64
	groups     map[string]*group // groupes filtrés, existants ou non
}

// sortedGroups renvoie les groupes filtrés ayant au moins un offset sur les
// topics filtrés, triés par nom.
func (s *state) sortedGroups() []*group {
	var gs []*group
	for _, g := range s.groups {
		if g.matched && len(g.offsets) > 0 {
			gs = append(gs, g)
		}
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].name < gs[j].name })
	return gs
}

func (s *state) group(name string) *group {
	if g, ok := s.groups[name]; ok {
		return g
	}
	g := &group{name: name, offsets: map[tp]int64{}}
	s.groups[name] = g
	return g
}

// load lit les topics et les groupes qui passent les filtres. extraGroups
// ajoute des groupes nommés explicitement, même absents du filtre ou du
// cluster (groupe destination encore inexistant).
func load(ctx context.Context, adm *kadm.Client, topics, groups filter, extraGroups ...string) (*state, error) {
	s := &state{start: map[tp]int64{}, end: map[tp]int64{}, groups: map[string]*group{}}

	td, err := adm.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	var names []string
	for _, t := range td {
		if !topics.match(t.Topic) {
			continue
		}
		if t.Err != nil {
			return nil, fmt.Errorf("topic %s: %w", t.Topic, t.Err)
		}
		names = append(names, t.Topic)
		for p := range t.Partitions {
			s.partitions = append(s.partitions, tp{t.Topic, p})
		}
	}
	sort.Slice(s.partitions, func(i, j int) bool { return s.partitions[i].less(s.partitions[j]) })
	if len(names) == 0 {
		return s, nil
	}
	known := map[string]bool{}
	for _, name := range names {
		known[name] = true
	}

	for _, l := range []struct {
		dst  map[tp]int64
		list func(context.Context, ...string) (kadm.ListedOffsets, error)
	}{{s.start, adm.ListStartOffsets}, {s.end, adm.ListEndOffsets}} {
		lo, err := l.list(ctx, names...)
		if err != nil {
			return nil, fmt.Errorf("list offsets: %w", err)
		}
		if err := lo.Error(); err != nil {
			return nil, fmt.Errorf("list offsets: %w", err)
		}
		lo.Each(func(o kadm.ListedOffset) { l.dst[tp{o.Topic, o.Partition}] = o.Offset })
	}

	listed, err := adm.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	for _, lg := range listed {
		// Sans état (Kafka < 2.6), un groupe actif passerait pour inactif.
		if lg.State == "" {
			return nil, fmt.Errorf("group %s: state unknown, kcg requires Kafka 2.6 or later", lg.Group)
		}
		if groups != nil && groups.match(lg.Group) {
			g := s.group(lg.Group)
			g.state, g.matched = lg.State, true
		}
	}
	for _, name := range extraGroups {
		g := s.group(name)
		if lg, ok := listed[name]; ok {
			g.state = lg.State
		}
	}
	if len(s.groups) == 0 {
		return s, nil
	}

	var gnames []string
	for name := range s.groups {
		gnames = append(gnames, name)
	}
	for name, r := range adm.FetchManyOffsets(ctx, gnames...) {
		if r.Err != nil {
			if errors.Is(r.Err, context.Canceled) || errors.Is(r.Err, context.DeadlineExceeded) {
				return nil, r.Err
			}
			return nil, fmt.Errorf("fetch offsets of group %s: %w", name, r.Err)
		}
		g := s.groups[name]
		var perr error
		r.Fetched.Each(func(o kadm.OffsetResponse) {
			if !known[o.Topic] {
				return
			}
			if o.Err != nil {
				perr = fmt.Errorf("fetch offsets of group %s on %s/%d: %w", name, o.Topic, o.Partition, o.Err)
			} else if o.At >= 0 {
				g.offsets[tp{o.Topic, o.Partition}] = o.At
			}
		})
		// Un offset illisible compté comme absent fausserait la sauvegarde.
		if perr != nil {
			return nil, perr
		}
	}
	return s, nil
}
