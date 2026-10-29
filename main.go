// Commande kcg : lit et modifie les offsets des consumer groups Kafka.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const usage = `Usage: kcg <command> --bootstrap-server host:port [--command-config file] [flags]

Commands:
  read     show committed offsets, one row per partition, one column per group
  align    copy offsets: --from oldest|newest|earliest|latest|<offset>|<group> --to <group>|all
  delete   delete the offsets of --group on --topic [--partition 0,1]
  clean    keep one reference group per topic and delete the others' offsets
  restore  restore offsets from a backup file: kcg restore <file.csv>

align, delete, clean and restore only print their plan unless --execute is set.
Run "kcg <command> -h" for the flags of a command, "kcg -version" for the version.
`

// version est injectée par -ldflags (make build, make dist) ; à défaut, celle
// du module pour un binaire installé par go install.
var version = "dev"

// moduleVersion garde la version injectée, sinon prend celle des informations
// de build quand elles en portent une (« (devel) » : compilation sans version).
func moduleVersion(injected string, info *debug.BuildInfo, ok bool) string {
	if injected != "dev" || !ok || info.Main.Version == "" || info.Main.Version == "(devel)" {
		return injected
	}
	return info.Main.Version
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run exécute une commande et renvoie le code de sortie.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if args[0] == "-version" || args[0] == "--version" || args[0] == "version" {
		info, ok := debug.ReadBuildInfo()
		fmt.Fprintln(stdout, "kcg", moduleVersion(version, info, ok))
		return 0
	}
	err := command(ctx, args[0], args[1:], stdout, stderr)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 2
	case errors.As(err, new(usageError)):
		fmt.Fprintf(stderr, "kcg %s: %v\n", args[0], err)
		return 2
	default:
		fmt.Fprintf(stderr, "kcg %s: %v\n", args[0], err)
		return 1
	}
}

// usageError signale une ligne de commande invalide (code de sortie 2).
type usageError string

func (e usageError) Error() string { return string(e) }

func usageErr(format string, a ...any) error {
	return usageError(fmt.Sprintf(format, a...))
}

type options struct {
	bootstrap, commandConfig string
	topic, group             string
	from, to, keep           string
	partitions               string
	execute, force           bool
}

func command(ctx context.Context, name string, args []string, stdout, stderr io.Writer) error {
	var o options
	fs := flag.NewFlagSet("kcg "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.bootstrap, "bootstrap-server", "", "comma-separated list of brokers (required)")
	fs.StringVar(&o.commandConfig, "command-config", "", "Kafka client .properties file (security.protocol, sasl.*, ssl.*)")
	write := name != "read"
	switch name {
	case "read", "align", "delete", "clean":
		def := ""
		if name == "read" {
			def = "*"
		}
		fs.StringVar(&o.topic, "topic", def, "topics: comma-separated names, wildcards * and ? allowed")
		fs.StringVar(&o.group, "group", def, "groups: comma-separated names, wildcards * and ? allowed")
	case "restore":
	default:
		fmt.Fprint(stderr, usage)
		return usageErr("unknown command %q", name)
	}
	switch name {
	case "align":
		fs.StringVar(&o.from, "from", "", "oldest|newest (min/max of the matched groups), earliest|latest (log), <offset> or <group>")
		fs.StringVar(&o.to, "to", "", "<group> or all (every matched group with offsets)")
	case "delete":
		fs.StringVar(&o.partitions, "partition", "", "comma-separated partitions (default: all)")
	case "clean":
		fs.StringVar(&o.keep, "keep", "", "group to keep when no group is clearly the reference")
	}
	if write {
		fs.BoolVar(&o.execute, "execute", false, "apply the plan (default: dry run)")
	}
	if name == "align" {
		fs.BoolVar(&o.force, "force", false, "allow moving offsets backward")
	}

	// restore accepte son fichier avant ou après les options.
	var file string
	if name == "restore" && len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		file, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if name == "restore" && file == "" && fs.NArg() > 0 {
		file = fs.Arg(0)
	} else if fs.NArg() > 0 {
		return usageErr("unexpected argument %q", fs.Arg(0))
	}
	if o.bootstrap == "" {
		return usageErr("--bootstrap-server is required")
	}

	var topics, groups filter
	var err error
	if name != "restore" {
		if o.topic == "" {
			return usageErr("--topic is required (use '*' for all topics)")
		}
		if topics, err = parseFilter(o.topic); err != nil {
			return usageErr("--topic: %v", err)
		}
		// align nomme ses groupes avec --from/--to : --group n'y est requis que
		// pour oldest, newest et all.
		needGroup := name != "align" || o.from == "oldest" || o.from == "newest" || o.to == "all"
		if o.group == "" && needGroup {
			return usageErr("--group is required (use '*' for all groups)")
		}
		if o.group != "" {
			if groups, err = parseFilter(o.group); err != nil {
				return usageErr("--group: %v", err)
			}
		}
	}
	if name == "align" && (o.from == "" || o.to == "") {
		return usageErr("--from and --to are required")
	}
	if name == "restore" && file == "" {
		return usageErr("backup file is required")
	}
	parts := map[int32]bool{}
	if o.partitions != "" {
		for _, s := range strings.Split(o.partitions, ",") {
			n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32)
			if err != nil || n < 0 {
				return usageErr("--partition: invalid partition %q", s)
			}
			parts[int32(n)] = true
		}
	}

	adm, err := connect(o)
	if err != nil {
		return err
	}
	defer adm.Close()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	var (
		s        *state
		cs       []change
		warnings []string
	)
	switch name {
	case "read":
		if s, err = load(ctx, adm, topics, groups); err != nil {
			return err
		}
		renderRead(stdout, s)
		return nil
	case "align":
		// Les groupes nommés par --from et --to sont lus même hors du filtre :
		// sans leurs offsets actuels, ni le contrôle de recul ni la
		// sauvegarde ne seraient justes.
		var extra []string
		if !sourceKeyword(o.from) {
			extra = append(extra, o.from)
		}
		if o.to != "all" {
			extra = append(extra, o.to)
		}
		if s, err = load(ctx, adm, topics, groups, extra...); err != nil {
			return err
		}
		if cs, err = alignChanges(s, o.from, o.to); err != nil {
			return err
		}
	case "delete":
		if s, err = load(ctx, adm, topics, groups); err != nil {
			return err
		}
		cs = deleteChanges(s, parts)
	case "clean":
		if s, err = load(ctx, adm, topics, groups); err != nil {
			return err
		}
		cs, warnings, err = cleanChanges(s, o.keep)
		for _, w := range warnings {
			fmt.Fprintln(stderr, "warning: "+w)
		}
		if err != nil {
			return err
		}
	case "restore":
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		es, err := readBackup(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
		var ts, gs []string
		for _, e := range es {
			ts = append(ts, e.p.topic)
			gs = append(gs, e.group)
		}
		tf := exact(ts...)
		if len(es) == 0 {
			fmt.Fprintln(stdout, "Backup is empty, nothing to restore.")
			return nil
		}
		if s, err = load(ctx, adm, tf, nil, gs...); err != nil {
			return err
		}
		if cs, err = restoreChanges(s, es); err != nil {
			return err
		}
		// restore annule une modification : un recul y est attendu.
		o.force = true
	}

	if len(cs) == 0 {
		fmt.Fprintln(stdout, "Nothing to change.")
		return nil
	}
	printPlan(stdout, cs)
	if err := check(s, cs, o.force); err != nil {
		return fmt.Errorf("refused, nothing written:\n  %w", err)
	}
	if !o.execute {
		fmt.Fprintf(stdout, "\nDry run: %d offset(s) would change. Re-run with --execute to apply.\n", len(cs))
		return nil
	}
	backup, err := writeBackup(cs, time.Now())
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "\nBackup of previous offsets: %s\nUndo with: kcg restore %s --execute\n", backup, backup)
	if err := apply(ctx, adm, cs); err != nil {
		return fmt.Errorf("%w (backup: %s)", err, backup)
	}
	fmt.Fprintf(stdout, "Applied %d offset change(s).\n", len(cs))
	return nil
}

// sourceKeyword indique si --from désigne autre chose qu'un groupe.
func sourceKeyword(s string) bool {
	switch s {
	case "oldest", "newest", "earliest", "latest":
		return true
	}
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func connect(o options) (*kadm.Client, error) {
	props := map[string]string{}
	if o.commandConfig != "" {
		f, err := os.Open(o.commandConfig)
		if err != nil {
			return nil, err
		}
		props, err = parseProperties(f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", o.commandConfig, err)
		}
	}
	opts, err := clientOptions(props)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", o.commandConfig, err)
	}
	opts = append(opts,
		kgo.SeedBrokers(splitList(o.bootstrap)...),
		kgo.ClientID("kcg"),
		kgo.DialTimeout(10*time.Second),
		kgo.RetryTimeout(30*time.Second),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return kadm.NewClient(cl), nil
}
