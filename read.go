package main

import (
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
)

// renderRead affiche une ligne par partition des topics où un groupe filtré a
// des offsets, une colonne par groupe. « * » marque le plus avancé de la
// partition, « ! » un offset sous log-start.
func renderRead(w io.Writer, s *state) {
	gs := s.sortedGroups()
	if len(gs) == 0 {
		fmt.Fprintln(w, "No committed offsets for the matched groups and topics.")
		return
	}
	used := map[string]bool{}
	for _, g := range gs {
		for p := range g.offsets {
			used[p.topic] = true
		}
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprint(tw, "TOPIC\tPARTITION\tLOG-START\tLOG-END")
	anyActive := false
	for _, g := range gs {
		name := g.name
		if g.active() {
			name += " (active)"
			anyActive = true
		}
		fmt.Fprint(tw, "\t"+name)
	}
	fmt.Fprintln(tw)

	anyBelow := false
	for _, p := range s.partitions {
		if !used[p.topic] {
			continue
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d", p.topic, p.partition, s.start[p], s.end[p])
		best := none
		for _, g := range gs {
			best = max(best, g.offset(p))
		}
		for _, g := range gs {
			o := g.offset(p)
			cell := "-"
			if o != none {
				cell = strconv.FormatInt(o, 10)
				if o < s.start[p] {
					cell += "!"
					anyBelow = true
				}
				if o == best {
					cell += "*"
				}
			}
			fmt.Fprint(tw, "\t"+cell)
		}
		fmt.Fprintln(tw)
	}
	tw.Flush()

	legend := "\n* most advanced group on the partition"
	if anyBelow {
		legend += ", ! below log-start (reset by auto.offset.reset on restart)"
	}
	if anyActive {
		legend += ", (active) group has members"
	}
	fmt.Fprintln(w, legend)
}
