package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// change décrit l'écriture prévue d'un offset ; none en old signifie qu'il n'y
// avait pas d'offset, none en new que l'offset est supprimé.
type change struct {
	group    string
	p        tp
	old, new int64
}

func sortChanges(cs []change) {
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].group != cs[j].group {
			return cs[i].group < cs[j].group
		}
		return cs[i].p.less(cs[j].p)
	})
}

// alignChanges calcule les écritures de align. from vaut oldest, newest,
// earliest, latest, un offset ou un nom de groupe ; to vaut all ou un nom de
// groupe. Une partition sans offset de référence est ignorée.
func alignChanges(s *state, from, to string) ([]change, error) {
	ref := map[tp]int64{}
	source := ""
	switch from {
	case "oldest", "newest":
		for _, g := range s.sortedGroups() {
			for p, o := range g.offsets {
				r, ok := ref[p]
				if !ok || (from == "oldest" && o < r) || (from == "newest" && o > r) {
					ref[p] = o
				}
			}
		}
	case "earliest", "latest":
		src := s.start
		if from == "latest" {
			src = s.end
		}
		for _, p := range s.partitions {
			ref[p] = src[p]
		}
	default:
		if n, err := strconv.ParseInt(from, 10, 64); err == nil {
			// Hors du log, le consumer repartirait selon auto.offset.reset.
			var out []string
			for _, p := range s.partitions {
				if n < s.start[p] || n > s.end[p] {
					out = append(out, fmt.Sprintf("%s/%d [%d, %d]", p.topic, p.partition, s.start[p], s.end[p]))
				}
				ref[p] = n
			}
			if len(out) > 0 {
				return nil, fmt.Errorf("--from %d is outside the log of:\n  %s", n, strings.Join(out, "\n  "))
			}
			break
		}
		source = from
		g := s.groups[from]
		if g == nil || len(g.offsets) == 0 {
			return nil, fmt.Errorf("group %s has no committed offsets on the matched topics", from)
		}
		for p, o := range g.offsets {
			ref[p] = o
		}
	}

	var dests []*group
	if to == "all" {
		for _, g := range s.sortedGroups() {
			if g.name != source {
				dests = append(dests, g)
			}
		}
		if len(dests) == 0 {
			return nil, errors.New("--to all: no matched group with committed offsets")
		}
	} else {
		if to == source {
			return nil, fmt.Errorf("--from and --to are the same group %s", to)
		}
		dests = []*group{s.group(to)}
	}

	var cs []change
	for _, g := range dests {
		for _, p := range s.partitions {
			r, ok := ref[p]
			if !ok || r == g.offset(p) {
				continue
			}
			cs = append(cs, change{g.name, p, g.offset(p), r})
		}
	}
	sortChanges(cs)
	return cs, nil
}

// deleteChanges supprime les offsets des groupes filtrés sur les topics
// filtrés, limités aux partitions données si la liste n'est pas vide.
func deleteChanges(s *state, partitions map[int32]bool) []change {
	var cs []change
	for _, g := range s.sortedGroups() {
		for p, o := range g.offsets {
			if len(partitions) == 0 || partitions[p.partition] {
				cs = append(cs, change{g.name, p, o, none})
			}
		}
	}
	sortChanges(cs)
	return cs
}

// cleanChanges garde, topic par topic, un seul groupe de référence parmi les
// groupes filtrés et supprime les offsets des autres :
//   - les groupes actifs sont gardés (avertissement si l'un d'eux est en retard) ;
//   - sinon le groupe désigné par keep ;
//   - sinon le plus avancé, s'il l'est sur toutes les partitions.
//
// En cas d'ambiguïté, l'erreur liste les partitions en conflit et rien n'est
// supprimé.
func cleanChanges(s *state, keep string) (cs []change, warnings []string, err error) {
	byTopic := map[string][]*group{}
	var topics []string
	for _, g := range s.sortedGroups() {
		seen := map[string]bool{}
		for p := range g.offsets {
			if !seen[p.topic] {
				seen[p.topic] = true
				if byTopic[p.topic] == nil {
					topics = append(topics, p.topic)
				}
				byTopic[p.topic] = append(byTopic[p.topic], g)
			}
		}
	}
	sort.Strings(topics)
	if keep != "" {
		if g := s.groups[keep]; g == nil || len(g.offsets) == 0 {
			return nil, nil, fmt.Errorf("--keep %s: group has no committed offsets on the matched topics", keep)
		}
	}

	var conflicts []string
	for _, t := range topics {
		gs := byTopic[t]
		if len(gs) < 2 {
			continue
		}
		var parts []tp
		for _, p := range s.partitions {
			if p.topic == t {
				parts = append(parts, p)
			}
		}
		best := map[tp]int64{}
		for _, p := range parts {
			best[p] = none
			for _, g := range gs {
				best[p] = max(best[p], g.offset(p))
			}
		}
		leads := func(g *group) bool {
			for _, p := range parts {
				if best[p] != none && g.offset(p) != best[p] {
					return false
				}
			}
			return true
		}

		kept := map[string]bool{}
		for _, g := range gs {
			if g.active() {
				kept[g.name] = true
				if !leads(g) {
					warnings = append(warnings, fmt.Sprintf("active group %s is not the most advanced on topic %s (%s)", g.name, t, leaders(parts, gs, best)))
				}
			}
		}
		if keep != "" && inGroups(gs, keep) {
			kept[keep] = true
		}
		if len(kept) == 0 {
			switch {
			case keep != "":
				conflicts = append(conflicts, fmt.Sprintf("topic %s: --keep %s has no offsets here", t, keep))
				continue
			default:
				var cands []string
				for _, g := range gs {
					if leads(g) {
						cands = append(cands, g.name)
					}
				}
				switch len(cands) {
				case 1:
					kept[cands[0]] = true
				case 0:
					conflicts = append(conflicts, fmt.Sprintf("topic %s: no group is the most advanced on every partition (%s)", t, leaders(parts, gs, best)))
					continue
				default:
					conflicts = append(conflicts, fmt.Sprintf("topic %s: groups %s have identical offsets", t, strings.Join(cands, ", ")))
					continue
				}
			}
		}
		for _, g := range gs {
			if kept[g.name] {
				continue
			}
			for _, p := range parts {
				if o := g.offset(p); o != none {
					cs = append(cs, change{g.name, p, o, none})
				}
			}
		}
	}
	if len(conflicts) > 0 {
		return nil, warnings, fmt.Errorf("clean refused, use --keep <group> to choose:\n  %s", strings.Join(conflicts, "\n  "))
	}
	sortChanges(cs)
	return cs, warnings, nil
}

func inGroups(gs []*group, name string) bool {
	for _, g := range gs {
		if g.name == name {
			return true
		}
	}
	return false
}

// leaders décrit, par partition, le ou les groupes les plus avancés.
func leaders(parts []tp, gs []*group, best map[tp]int64) string {
	var out []string
	for _, p := range parts {
		if best[p] == none {
			continue
		}
		var names []string
		for _, g := range gs {
			if g.offset(p) == best[p] {
				names = append(names, g.name)
			}
		}
		out = append(out, fmt.Sprintf("p%d: %s=%d", p.partition, strings.Join(names, "="), best[p]))
	}
	return strings.Join(out, ", ")
}

// check refuse un plan qui touche un groupe actif ou, sans force, qui recule
// un offset existant.
func check(s *state, cs []change, force bool) error {
	var errs []string
	seen := map[string]bool{}
	var back []string
	for _, c := range cs {
		g := s.groups[c.group]
		if g != nil && g.active() && !seen[c.group] {
			seen[c.group] = true
			errs = append(errs, fmt.Sprintf("group %s has active members (state %s): Kafka only accepts offset changes on inactive groups", c.group, g.state))
		}
		if !force && c.old != none && c.new != none && c.new < c.old {
			back = append(back, fmt.Sprintf("%s %s/%d %d -> %d", c.group, c.p.topic, c.p.partition, c.old, c.new))
		}
	}
	if len(back) > 0 {
		errs = append(errs, "moving offsets backward requires --force:\n    "+strings.Join(back, "\n    "))
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n  "))
	}
	return nil
}

func printPlan(w io.Writer, cs []change) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "GROUP\tTOPIC\tPARTITION\tCURRENT\tNEW")
	for _, c := range cs {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n", c.group, c.p.topic, c.p.partition, offsetText(c.old, "-"), offsetText(c.new, "(delete)"))
	}
	tw.Flush()
}

func offsetText(o int64, absent string) string {
	if o == none {
		return absent
	}
	return strconv.FormatInt(o, 10)
}

// writeBackup sauvegarde les offsets d'avant le plan (offset vide : aucun
// offset) dans un CSV du répertoire courant et renvoie son nom.
func writeBackup(cs []change, now time.Time) (string, error) {
	name := "kcg-backup-" + now.Format("20060102-150405.000") + ".csv"
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", fmt.Errorf("backup: %w", err)
	}
	w := csv.NewWriter(f)
	w.Write([]string{"group", "topic", "partition", "offset"})
	for _, c := range cs {
		old := ""
		if c.old != none {
			old = strconv.FormatInt(c.old, 10)
		}
		w.Write([]string{c.group, c.p.topic, strconv.Itoa(int(c.p.partition)), old})
	}
	w.Flush()
	if err := errors.Join(w.Error(), f.Close()); err != nil {
		return "", fmt.Errorf("backup: %w", err)
	}
	return name, nil
}

// backupEntry est une ligne de sauvegarde.
type backupEntry struct {
	group  string
	p      tp
	offset int64
}

func readBackup(r io.Reader) ([]backupEntry, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || strings.Join(rows[0], ",") != "group,topic,partition,offset" {
		return nil, errors.New("missing header group,topic,partition,offset")
	}
	var es []backupEntry
	for i, row := range rows[1:] {
		part, err := strconv.ParseInt(row[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("line %d: partition %q", i+2, row[2])
		}
		off := none
		if row[3] != "" {
			if off, err = strconv.ParseInt(row[3], 10, 64); err != nil || off < 0 {
				return nil, fmt.Errorf("line %d: offset %q", i+2, row[3])
			}
		}
		es = append(es, backupEntry{row[0], tp{row[1], int32(part)}, off})
	}
	return es, nil
}

// restoreChanges ramène chaque groupe de la sauvegarde à l'offset noté. Une
// partition introuvable fait tout refuser : la restauration serait partielle.
func restoreChanges(s *state, es []backupEntry) ([]change, error) {
	exists := map[tp]bool{}
	for _, p := range s.partitions {
		exists[p] = true
	}
	var missing []string
	for _, e := range es {
		if !exists[e.p] {
			missing = append(missing, fmt.Sprintf("%s/%d", e.p.topic, e.p.partition))
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("partitions not found in the cluster: %s", strings.Join(missing, ", "))
	}
	var cs []change
	for _, e := range es {
		cur := s.group(e.group).offset(e.p)
		if cur != e.offset {
			cs = append(cs, change{e.group, e.p, cur, e.offset})
		}
	}
	sortChanges(cs)
	return cs, nil
}

// apply écrit le plan groupe par groupe : commits, puis suppressions.
func apply(ctx context.Context, adm *kadm.Client, cs []change) error {
	commits := map[string]kadm.Offsets{}
	deletes := map[string]kadm.TopicsSet{}
	var order []string
	for _, c := range cs {
		if commits[c.group] == nil && deletes[c.group] == nil {
			order = append(order, c.group)
			commits[c.group] = kadm.Offsets{}
			deletes[c.group] = kadm.TopicsSet{}
		}
		if c.new == none {
			ts := deletes[c.group]
			ts.Add(c.p.topic, c.p.partition)
			deletes[c.group] = ts
		} else {
			os := commits[c.group]
			os.AddOffset(c.p.topic, c.p.partition, c.new, -1)
			commits[c.group] = os
		}
	}
	for _, g := range order {
		if len(commits[g]) > 0 {
			r, err := adm.CommitOffsets(ctx, g, commits[g])
			if err == nil {
				err = r.Error()
			}
			if err != nil {
				return fmt.Errorf("commit offsets of group %s: %w", g, err)
			}
		}
		if len(deletes[g]) > 0 {
			r, err := adm.DeleteOffsets(ctx, g, deletes[g])
			if err == nil {
				err = r.Error()
			}
			if err != nil {
				return fmt.Errorf("delete offsets of group %s: %w", g, err)
			}
		}
	}
	return nil
}
