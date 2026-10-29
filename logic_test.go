package main

import (
	"bytes"
	"os"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

// testState construit un état : topic « t » à n partitions (log 10..100) et
// des groupes dont les offsets sont donnés par partition (none = absent).
func testState(n int, groups map[string][]int64, active ...string) *state {
	s := &state{start: map[tp]int64{}, end: map[tp]int64{}, groups: map[string]*group{}}
	for p := range int32(n) {
		s.partitions = append(s.partitions, tp{"t", p})
		s.start[tp{"t", p}] = 10
		s.end[tp{"t", p}] = 100
	}
	for name, offs := range groups {
		g := s.group(name)
		g.matched, g.state = true, "Empty"
		for p, o := range offs {
			if o != none {
				g.offsets[tp{"t", int32(p)}] = o
			}
		}
	}
	for _, name := range active {
		s.groups[name].state = "Stable"
	}
	return s
}

// planText résume un plan en « groupe:partition:ancien>nouveau ».
func planText(cs []change) string {
	var out []string
	for _, c := range cs {
		out = append(out, c.group+":"+offsetText(int64(c.p.partition), "")+":"+offsetText(c.old, "-")+">"+offsetText(c.new, "x"))
	}
	return strings.Join(out, " ")
}

func TestAlign(t *testing.T) {
	groups := map[string][]int64{
		"g1": {50, 60, none},
		"g2": {40, 70, 20},
		"g3": {none, none, none},
	}
	for _, c := range []struct{ from, to, want string }{
		{"g1", "g2", "g2:0:40>50 g2:1:70>60"},
		{"g1", "g3", "g3:0:->50 g3:1:->60"},
		{"g1", "new", "new:0:->50 new:1:->60"},
		{"oldest", "all", "g1:0:50>40 g1:2:->20 g2:1:70>60"},
		{"newest", "all", "g1:1:60>70 g1:2:->20 g2:0:40>50"},
		{"earliest", "g1", "g1:0:50>10 g1:1:60>10 g1:2:->10"},
		{"latest", "g2", "g2:0:40>100 g2:1:70>100 g2:2:20>100"},
		{"55", "g1", "g1:0:50>55 g1:1:60>55 g1:2:->55"},
		{"g2", "all", "g1:0:50>40 g1:1:60>70 g1:2:->20"},
	} {
		cs, err := alignChanges(testState(3, groups), c.from, c.to)
		if err != nil {
			t.Fatalf("%s -> %s : %v", c.from, c.to, err)
		}
		if got := planText(cs); got != c.want {
			t.Errorf("%s -> %s : %s, attendu %s", c.from, c.to, got, c.want)
		}
	}
	// Offset hors de [log-start, log-end] = [10, 100] : refusé.
	for _, c := range [][2]string{{"g1", "g1"}, {"absent", "g2"}, {"-3", "g2"}, {"5", "g2"}, {"101", "g2"}} {
		if _, err := alignChanges(testState(3, groups), c[0], c[1]); err == nil {
			t.Errorf("%s -> %s accepté", c[0], c[1])
		}
	}
}

func TestCheck(t *testing.T) {
	s := testState(2, map[string][]int64{"g1": {50, 50}, "g2": {60, 40}, "g3": {1, 1}}, "g3")
	cs, _ := alignChanges(s, "g1", "g2")
	if err := check(s, cs, false); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Errorf("recul sans --force : %v", err)
	}
	if err := check(s, cs, true); err != nil {
		t.Errorf("recul avec --force : %v", err)
	}
	cs, _ = alignChanges(s, "g1", "g3")
	if err := check(s, cs, true); err == nil || !strings.Contains(err.Error(), "active") {
		t.Errorf("groupe actif : %v", err)
	}
}

func TestClean(t *testing.T) {
	for _, c := range []struct {
		name    string
		groups  map[string][]int64
		active  []string
		keep    string
		want    string
		warn    bool
		refused string
	}{
		{name: "actif en retard", groups: map[string][]int64{"g1": {10, 10}, "g2": {50, 50}}, active: []string{"g1"}, want: "g2:0:50>x g2:1:50>x", warn: true},
		{name: "actif en tête", groups: map[string][]int64{"g1": {50, 50}, "g2": {10, 10}}, active: []string{"g1"}, want: "g2:0:10>x g2:1:10>x"},
		{name: "plus avancé partout", groups: map[string][]int64{"g1": {50, 30}, "g2": {50, 20}, "g3": {none, 30}}, want: "g2:0:50>x g2:1:20>x g3:1:30>x"},
		{name: "conflit", groups: map[string][]int64{"g1": {50, 10}, "g2": {10, 50}}, refused: "p0: g1=50, p1: g2=50"},
		{name: "identiques", groups: map[string][]int64{"g1": {50, 50}, "g2": {50, 50}}, refused: "identical"},
		{name: "keep", groups: map[string][]int64{"g1": {50, 10}, "g2": {10, 50}, "g3": {1, 1}}, keep: "g2", want: "g1:0:50>x g1:1:10>x g3:0:1>x g3:1:1>x"},
		{name: "seul", groups: map[string][]int64{"g1": {50, 10}}, want: ""},
	} {
		cs, warns, err := cleanChanges(testState(2, c.groups, c.active...), c.keep)
		if c.refused != "" {
			if err == nil || !strings.Contains(err.Error(), c.refused) || cs != nil {
				t.Errorf("%s : refus attendu contenant %q, obtenu %v / %v", c.name, c.refused, err, cs)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s : %v", c.name, err)
		}
		if got := planText(cs); got != c.want {
			t.Errorf("%s : %s, attendu %s", c.name, got, c.want)
		}
		if (len(warns) > 0) != c.warn {
			t.Errorf("%s : avertissements %v", c.name, warns)
		}
	}
}

func TestRenderRead(t *testing.T) {
	s := testState(2, map[string][]int64{"g1": {5, 60}, "g2": {50, 60}}, "g2")
	var b bytes.Buffer
	renderRead(&b, s)
	out := b.String()
	for _, want := range []string{"g2 (active)", "5!", "50*", "60*", "below log-start", "(active) group has members"} {
		if !strings.Contains(out, want) {
			t.Errorf("sortie sans %q :\n%s", want, out)
		}
	}
	if lines := strings.Count(out, "\nt "); lines != 2 {
		t.Errorf("%d lignes de partition :\n%s", lines, out)
	}
}

func TestModuleVersion(t *testing.T) {
	info := &debug.BuildInfo{Main: debug.Module{Version: "v0.1.0"}}
	devel := &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}
	for _, c := range []struct {
		injected string
		info     *debug.BuildInfo
		ok       bool
		want     string
	}{
		{"v0.2.0", info, true, "v0.2.0"},
		{"dev", info, true, "v0.1.0"},
		{"dev", devel, true, "dev"},
		{"dev", nil, false, "dev"},
	} {
		if got := moduleVersion(c.injected, c.info, c.ok); got != c.want {
			t.Errorf("moduleVersion(%q) = %q, attendu %q", c.injected, got, c.want)
		}
	}
}

func TestBackupRoundTrip(t *testing.T) {
	t.Chdir(t.TempDir())
	before := testState(2, map[string][]int64{"g1": {50, 60}, "g2": {40, none}})
	cs, _ := alignChanges(before, "g1", "g2")
	name, err := writeBackup(cs, time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(name)
	if string(data) != "group,topic,partition,offset\ng2,t,0,40\ng2,t,1,\n" {
		t.Errorf("sauvegarde inattendue :\n%s", data)
	}
	es, err := readBackup(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	after := testState(2, map[string][]int64{"g1": {50, 60}, "g2": {50, 60}})
	cs, err = restoreChanges(after, es)
	if got := planText(cs); err != nil || got != "g2:0:50>40 g2:1:60>x" {
		t.Errorf("restore : %s %v", got, err)
	}
	es = append(es, backupEntry{"g2", tp{"gone", 0}, 5})
	if _, err := restoreChanges(after, es); err == nil || !strings.Contains(err.Error(), "gone/0") {
		t.Errorf("partition absente : %v", err)
	}
	if _, err := readBackup(strings.NewReader("a,b\n")); err == nil {
		t.Error("en-tête invalide accepté")
	}
}
