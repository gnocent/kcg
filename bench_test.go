//go:build bench

// Recette sur le banc Docker de kafkabeat : go test -tags bench.
// KCG_KAFKA39 et KCG_KAFKA44 surchargent les adresses par défaut.
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var clusters = []struct{ name, env, addr string }{
	{"3.9", "KCG_KAFKA39", "localhost:19092"},
	{"4.4", "KCG_KAFKA44", "localhost:19094"},
}

type bench struct {
	t      *testing.T
	addr   string
	cl     *kgo.Client
	adm    *kadm.Client
	prefix string
	topics []string
	groups []string
}

// eachKafka lance f sur chaque cluster du banc, dans un répertoire temporaire
// qui reçoit les sauvegardes.
func eachKafka(t *testing.T, f func(t *testing.T, b *bench)) {
	for _, c := range clusters {
		t.Run(c.name, func(t *testing.T) {
			addr := c.addr
			if v := os.Getenv(c.env); v != "" {
				addr = v
			}
			f(t, newBench(t, addr))
		})
	}
}

func newBench(t *testing.T, addr string) *bench {
	t.Chdir(t.TempDir())
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	name := regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(t.Name(), "-")
	b := &bench{t: t, addr: addr, cl: cl, adm: kadm.NewClient(cl), prefix: fmt.Sprintf("kcg-%s-%s", time.Now().Format("150405"), strings.ToLower(name))}
	t.Cleanup(func() {
		ctx := context.Background()
		if len(b.groups) > 0 {
			b.adm.DeleteGroups(ctx, b.groups...)
		}
		if len(b.topics) > 0 {
			b.adm.DeleteTopics(ctx, b.topics...)
		}
		cl.Close()
	})
	return b
}

// topic crée un topic de 6 partitions et y produit 100 messages par partition.
func (b *bench) topic(name string) string {
	b.t.Helper()
	ctx := context.Background()
	name = b.prefix + "-" + name
	if _, err := b.adm.CreateTopic(ctx, 6, 1, nil, name); err != nil {
		b.t.Fatal(err)
	}
	b.topics = append(b.topics, name)
	var recs []*kgo.Record
	for p := range int32(6) {
		for range 100 {
			recs = append(recs, &kgo.Record{Topic: name, Partition: p, Value: []byte("x")})
		}
	}
	if err := b.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		b.t.Fatal(err)
	}
	return name
}

func (b *bench) group(name string) string {
	g := b.prefix + "-" + name
	b.groups = append(b.groups, g)
	return g
}

// commit écrit les offsets d'un groupe, partition par partition (none : rien).
func (b *bench) commit(group, topic string, offs ...int64) {
	b.t.Helper()
	var os kadm.Offsets
	for p, o := range offs {
		if o != none {
			os.AddOffset(topic, int32(p), o, -1)
		}
	}
	if err := b.adm.CommitAllOffsets(context.Background(), group, os); err != nil {
		b.t.Fatal(err)
	}
}

// offsets lit les offsets committés d'un groupe sur les 6 partitions.
func (b *bench) offsets(group, topic string) []int64 {
	b.t.Helper()
	r, err := b.adm.FetchOffsetsForTopics(context.Background(), group, topic)
	if err != nil {
		b.t.Fatal(err)
	}
	out := []int64{none, none, none, none, none, none}
	r.Each(func(o kadm.OffsetResponse) {
		if o.Err == nil && o.At >= 0 {
			out[o.Partition] = o.At
		}
	})
	return out
}

func (b *bench) expectOffsets(group, topic string, want ...int64) {
	b.t.Helper()
	if got := b.offsets(group, topic); fmt.Sprint(got) != fmt.Sprint(want) {
		b.t.Fatalf("offsets de %s : %v, attendu %v", group, got, want)
	}
}

// consumer rend un groupe actif (protocole classic, ou consumer KIP-848 si
// serverSide) sans committer, et attend qu'il soit stable.
func (b *bench) consumer(group, topic string, serverSide bool) {
	b.t.Helper()
	opts := []kgo.Opt{kgo.SeedBrokers(b.addr), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.DisableAutoCommit()}
	if serverSide {
		opts = append(opts, kgo.ServerSideBalancer())
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		b.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			cl.PollFetches(ctx)
		}
	}()
	b.t.Cleanup(func() {
		cancel()
		<-done
		cl.Close()
		b.waitState(group, "Empty")
	})
	b.waitState(group, "Stable")
}

func (b *bench) waitState(group, state string) {
	b.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		ls, err := b.adm.ListGroups(context.Background())
		if err == nil && ls[group].State == state {
			return
		}
		if time.Now().After(deadline) {
			b.t.Fatalf("groupe %s jamais %s (état %q)", group, state, ls[group].State)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// kcg lance la commande dans le processus de test.
func (b *bench) kcg(args ...string) (string, string, int) {
	var out, errb bytes.Buffer
	code := run(context.Background(), append(args, "--bootstrap-server", b.addr), &out, &errb)
	return out.String(), errb.String(), code
}

func (b *bench) kcgOK(args ...string) string {
	b.t.Helper()
	out, errs, code := b.kcg(args...)
	if code != 0 {
		b.t.Fatalf("kcg %v : code %d\n%s%s", args, code, out, errs)
	}
	return out
}

func backups(t *testing.T) []string {
	t.Helper()
	m, _ := filepath.Glob("kcg-backup-*.csv")
	return m
}

// rows renvoie les cellules des lignes de read pour un topic.
func rows(out, topic string) [][]string {
	var rs [][]string
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, topic+" ") {
			rs = append(rs, strings.Fields(l))
		}
	}
	return rs
}

func header(out string) string {
	return strings.SplitN(out, "\n", 2)[0]
}

// C1
func TestRead(t *testing.T) {
	eachKafka(t, func(t *testing.T, b *bench) {
		topic := b.topic("t")
		other := b.topic("u")
		g1, g2, g3 := b.group("g1"), b.group("g2"), b.group("g3")
		b.commit(g1, topic, 10, 20, 30, 40, 50, 60)
		b.commit(g2, topic, 60, 50, 40, 30, 20, 10)
		b.commit(g3, topic, 5, 5, 5, 5, 5, 100)
		b.commit(g1, other, 1, 1, 1, 1, 1, 1)
		var del kadm.Offsets
		del.AddOffset(topic, 0, 8, -1)
		if _, err := b.adm.DeleteRecords(context.Background(), del); err != nil {
			t.Fatal(err)
		}
		b.consumer(g3, topic, false)

		out := b.kcgOK("read", "--topic", topic, "--group", b.prefix+"-g*")
		rs := rows(out, topic)
		if len(rs) != 6 {
			t.Fatalf("%d lignes, attendu 6 :\n%s", len(rs), out)
		}
		h := header(out)
		if !strings.Contains(h, g1) || !strings.Contains(h, g2) || !strings.Contains(h, g3+" (active)") || strings.Contains(h, g1+" (active)") {
			t.Fatalf("en-tête inattendu : %s", h)
		}
		// Colonnes : topic, partition, log-start, log-end, g1, g2, g3.
		want := [][]string{
			{"8", "100", "10", "60*", "5!"},
			{"0", "100", "20", "50*", "5"},
			{"0", "100", "30", "40*", "5"},
			{"0", "100", "40*", "30", "5"},
			{"0", "100", "50*", "20", "5"},
			{"0", "100", "60", "10", "100*"},
		}
		for i, r := range rs {
			if fmt.Sprint(r[2:]) != fmt.Sprint(want[i]) {
				t.Errorf("partition %d : %v, attendu %v\n%s", i, r[2:], want[i], out)
			}
		}

		out = b.kcgOK("read", "--topic", topic, "--group", g1+","+g3)
		if h := header(out); strings.Contains(h, g2) || !strings.Contains(h, g1) || !strings.Contains(h, g3) {
			t.Errorf("filtre liste : %s", h)
		}
		out = b.kcgOK("read", "--topic", b.prefix+"-*", "--group", g1)
		if len(rows(out, topic)) != 6 || len(rows(out, other)) != 6 {
			t.Errorf("filtre joker sur les topics :\n%s", out)
		}
	})
}

// C2
func TestAlignAndRestore(t *testing.T) {
	eachKafka(t, func(t *testing.T, b *bench) {
		topic := b.topic("t")
		g1, g2 := b.group("g1"), b.group("g2")
		b.commit(g1, topic, 50, 50, 50, 50, 50, 50)
		b.commit(g2, topic, 10, 20, 30, 40, 50, none)

		out := b.kcgOK("align", "--topic", topic, "--from", g1, "--to", g2)
		if !strings.Contains(out, "Dry run: 5 offset(s)") {
			t.Fatalf("simulation inattendue :\n%s", out)
		}
		b.expectOffsets(g2, topic, 10, 20, 30, 40, 50, none)
		if len(backups(t)) != 0 {
			t.Fatal("sauvegarde écrite en simulation")
		}

		b.kcgOK("align", "--topic", topic, "--from", g1, "--to", g2, "--execute")
		b.expectOffsets(g2, topic, 50, 50, 50, 50, 50, 50)
		bk := backups(t)
		if len(bk) != 1 {
			t.Fatalf("sauvegardes : %v", bk)
		}

		b.kcgOK("restore", bk[0], "--execute")
		b.expectOffsets(g2, topic, 10, 20, 30, 40, 50, none)
		b.expectOffsets(g1, topic, 50, 50, 50, 50, 50, 50)
	})
}

// Groupe destination au nom numérique : ses offsets actuels doivent être lus
// (contrôle de recul, sauvegarde fidèle).
func TestAlignToNumericGroup(t *testing.T) {
	eachKafka(t, func(t *testing.T, b *bench) {
		topic := b.topic("t")
		g1 := b.group("g1")
		num := fmt.Sprint(time.Now().UnixNano() % 1e12)
		b.groups = append(b.groups, num)
		b.commit(g1, topic, 50, 50, 50, 50, 50, 50)
		b.commit(num, topic, 60, 60, 60, 60, 60, 60)

		out, errs, code := b.kcg("align", "--topic", topic, "--from", g1, "--to", num, "--execute")
		if code != 1 || !strings.Contains(errs, "--force") {
			t.Fatalf("recul vers %s sans --force : code %d\n%s%s", num, code, out, errs)
		}
		b.kcgOK("align", "--topic", topic, "--from", g1, "--to", num, "--force", "--execute")
		b.expectOffsets(num, topic, 50, 50, 50, 50, 50, 50)
		b.kcgOK("restore", backups(t)[0], "--execute")
		b.expectOffsets(num, topic, 60, 60, 60, 60, 60, 60)
	})
}

// C3
func TestAlignSources(t *testing.T) {
	eachKafka(t, func(t *testing.T, b *bench) {
		topic := b.topic("t")
		g1, g2, g3 := b.group("g1"), b.group("g2"), b.group("g3")
		set := func() {
			b.commit(g1, topic, 10, 60, 30, 30, 30, 30)
			b.commit(g2, topic, 20, 50, 30, 30, 30, 30)
			b.commit(g3, topic, 30, 40, 30, 30, 30, none)
		}
		all := b.prefix + "-g*"

		set()
		b.kcgOK("align", "--topic", topic, "--group", all, "--from", "oldest", "--to", "all", "--force", "--execute")
		for _, g := range []string{g1, g2} {
			b.expectOffsets(g, topic, 10, 40, 30, 30, 30, 30)
		}
		b.expectOffsets(g3, topic, 10, 40, 30, 30, 30, 30)

		set()
		b.kcgOK("align", "--topic", topic, "--group", all, "--from", "newest", "--to", "all", "--execute")
		for _, g := range []string{g1, g2, g3} {
			b.expectOffsets(g, topic, 30, 60, 30, 30, 30, 30)
		}

		var del kadm.Offsets
		del.AddOffset(topic, 1, 25, -1)
		if _, err := b.adm.DeleteRecords(context.Background(), del); err != nil {
			t.Fatal(err)
		}
		b.kcgOK("align", "--topic", topic, "--from", "earliest", "--to", g1, "--force", "--execute")
		b.expectOffsets(g1, topic, 0, 25, 0, 0, 0, 0)
		b.kcgOK("align", "--topic", topic, "--from", "latest", "--to", g1, "--execute")
		b.expectOffsets(g1, topic, 100, 100, 100, 100, 100, 100)
		b.kcgOK("align", "--topic", topic, "--from", "42", "--to", g1, "--force", "--execute")
		b.expectOffsets(g1, topic, 42, 42, 42, 42, 42, 42)
	})
}

// C4
func TestRefusals(t *testing.T) {
	eachKafka(t, func(t *testing.T, b *bench) {
		topic := b.topic("t")
		g1, g2, g3 := b.group("g1"), b.group("g2"), b.group("g3")
		b.commit(g1, topic, 50, 50, 50, 50, 50, 50)
		b.commit(g2, topic, 60, 40, 50, 50, 50, 50)
		b.commit(g3, topic, 10, 10, 10, 10, 10, 10)
		b.consumer(g3, topic, false)

		out, errs, code := b.kcg("align", "--topic", topic, "--from", g1, "--to", g3, "--execute")
		if code != 1 || !strings.Contains(errs, "active members") {
			t.Fatalf("destination active : code %d\n%s%s", code, out, errs)
		}
		b.expectOffsets(g3, topic, 10, 10, 10, 10, 10, 10)
		if len(backups(t)) != 0 {
			t.Fatal("sauvegarde écrite malgré le refus")
		}

		out, errs, code = b.kcg("align", "--topic", topic, "--from", g1, "--to", g2, "--execute")
		if code != 1 || !strings.Contains(errs, "--force") || !strings.Contains(errs, g2+" "+topic+"/0 60 -> 50") {
			t.Fatalf("recul sans --force : code %d\n%s%s", code, out, errs)
		}
		b.expectOffsets(g2, topic, 60, 40, 50, 50, 50, 50)

		b.kcgOK("align", "--topic", topic, "--from", g1, "--to", g2, "--force", "--execute")
		b.expectOffsets(g2, topic, 50, 50, 50, 50, 50, 50)
	})
}

// C5, et delete ciblé
func TestDeleteAndClean(t *testing.T) {
	eachKafka(t, func(t *testing.T, b *bench) {
		topic := b.topic("t")
		g1, g2, g3 := b.group("g1"), b.group("g2"), b.group("g3")
		all := b.prefix + "-g*"

		// delete ciblé
		b.commit(g1, topic, 50, 50, 50, 50, 50, 50)
		b.kcgOK("delete", "--topic", topic, "--group", g1, "--partition", "0,1", "--execute")
		b.expectOffsets(g1, topic, none, none, 50, 50, 50, 50)

		// tous inactifs, ambigu : refus, rien supprimé
		b.commit(g1, topic, 50, 10, 50, 50, 50, 50)
		b.commit(g2, topic, 10, 50, 50, 50, 50, 50)
		out, errs, code := b.kcg("clean", "--topic", topic, "--group", all, "--execute")
		if code != 1 || !strings.Contains(errs, "p0: "+g1+"=50") || !strings.Contains(errs, "p1: "+g2+"=50") {
			t.Fatalf("clean ambigu : code %d\n%s%s", code, out, errs)
		}
		b.expectOffsets(g1, topic, 50, 10, 50, 50, 50, 50)
		b.expectOffsets(g2, topic, 10, 50, 50, 50, 50, 50)

		// --keep tranche
		b.kcgOK("clean", "--topic", topic, "--group", all, "--keep", g2, "--execute")
		b.expectOffsets(g1, topic, none, none, none, none, none, none)
		b.expectOffsets(g2, topic, 10, 50, 50, 50, 50, 50)

		// actif en retard + inactif en avance : inactif supprimé, avertissement
		b.commit(g3, topic, 5, 5, 5, 5, 5, 5)
		b.consumer(g3, topic, false)
		out, errs, code = b.kcg("clean", "--topic", topic, "--group", all, "--execute")
		if code != 0 || !strings.Contains(errs, "warning: active group "+g3+" is not the most advanced") {
			t.Fatalf("clean avec actif : code %d\n%s%s", code, out, errs)
		}
		b.expectOffsets(g2, topic, none, none, none, none, none, none)
		b.expectOffsets(g3, topic, 5, 5, 5, 5, 5, 5)
		if n := len(backups(t)); n != 3 {
			t.Errorf("%d sauvegardes, attendu 3", n)
		}
	})
}

// C6 : groupe KIP-848 (Kafka 4.4)
func TestKIP848(t *testing.T) {
	addr := clusters[1].addr
	if v := os.Getenv(clusters[1].env); v != "" {
		addr = v
	}
	b := newBench(t, addr)
	topic := b.topic("t")
	g1, g2 := b.group("g1"), b.group("g2")
	b.commit(g1, topic, 50, 50, 50, 50, 50, 50)
	b.commit(g2, topic, 10, 10, 10, 10, 10, 10)
	b.consumer(g2, topic, true)

	d, err := b.adm.DescribeConsumerGroups(context.Background(), g2)
	if err != nil || d[g2].Err != nil || len(d[g2].Members) == 0 {
		t.Fatalf("%s n'est pas un groupe KIP-848 actif : %v %+v", g2, err, d[g2])
	}

	out := b.kcgOK("read", "--topic", topic, "--group", b.prefix+"-g*")
	if !strings.Contains(header(out), g2+" (active)") || len(rows(out, topic)) != 6 {
		t.Fatalf("lecture du groupe KIP-848 :\n%s", out)
	}
	out, errs, code := b.kcg("align", "--topic", topic, "--from", g1, "--to", g2, "--execute")
	if code != 1 || !strings.Contains(errs, "active members") {
		t.Fatalf("écriture sur groupe KIP-848 actif : code %d\n%s%s", code, out, errs)
	}
	b.expectOffsets(g2, topic, 10, 10, 10, 10, 10, 10)
}
