package seqkit

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/bio/util"
	"github.com/shenwei356/util/math"
	"github.com/tatsushid/go-prettytable"
	"ignis/driver/api"
	"strings"
)

type SeqKitStatsOptions struct {
	inner StatsOptions
}

type StatsOptions struct {
	Config     KitConfig
	Tabular    *bool
	GapLetters *string
	All        *bool
	SkipErr    *bool
	FqEncoding *string
	Basename   *bool
}

func (this *StatsOptions) setDefaults() *StatsOptions {
	this.Config.setDefaults()
	setDefault(&this.Tabular, false)
	setDefault(&this.GapLetters, "- .")
	setDefault(&this.All, false)
	setDefault(&this.SkipErr, false)
	setDefault(&this.FqEncoding, "sanger")
	setDefault(&this.Basename, false)

	return this
}

func (this *SeqKitStatsOptions) Config(v *SeqKitConfig) *SeqKitStatsOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitStatsOptions) Tabular(v bool) *SeqKitStatsOptions {
	this.inner.Tabular = &v
	return this
}

func (this *SeqKitStatsOptions) GapLetters(v string) *SeqKitStatsOptions {
	this.inner.GapLetters = &v
	return this
}

func (this *SeqKitStatsOptions) All(v bool) *SeqKitStatsOptions {
	this.inner.All = &v
	return this
}

func (this *SeqKitStatsOptions) SkipErr(v bool) *SeqKitStatsOptions {
	this.inner.SkipErr = &v
	return this
}

func (this *SeqKitStatsOptions) FqEncoding(v string) *SeqKitStatsOptions {
	this.inner.FqEncoding = &v
	return this
}

func (this *SeqKitStatsOptions) Basename(v bool) *SeqKitStatsOptions {
	this.inner.Basename = &v
	return this
}

func Stats(name string, format string, input *api.IDataFrame[string], o *SeqKitStatsOptions) (*StatInfo, error) {
	if o == nil {
		o = &SeqKitStatsOptions{}
	}
	opts := o.inner
	opts.setDefaults()
	libprepare, err := api.AddParam(libSource("Stats"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	statsCount, err := api.MapPartitions[string, map[int64]int64](input, libprepare)
	if err != nil {
		return nil, err
	}

	stats, err := statsCount.Reduce(libSource("StatsReduce"))
	if err != nil {
		return nil, err
	}

	Q20 := int64(-1)
	Q30 := int64(-2)
	GAP_SUM := int64(-3)
	T := int64(-4)

	q20 := stats[Q20]
	delete(stats, Q20)
	q30 := stats[Q30]
	delete(stats, Q30)
	gapSum := uint64(stats[GAP_SUM])
	delete(stats, GAP_SUM)
	ti := stats[T]
	delete(stats, T)
	var t string
	if ti == int64('D') {
		t = "DNA"
	} else if ti == int64('R') {
		t = "RNA"
	} else if ti == int64('U') {
		t = ""
	} else {
		firstSeq, err := input.Take(1)
		if err != nil {
			return nil, err
		}
		fastxReader, err := fastx.NewReaderFromIO(nil, strings.NewReader(firstSeq[0]), *opts.Config.IDRegexp)
		if err != nil {
			return nil, err
		}
		_, err = fastxReader.Read()
		if err != nil {
			return nil, err
		}
		t = fastxReader.Alphabet().String()
	}

	lensStats := util.NewLengthStats()

	for k, v := range stats {
		for i := int64(0); i < v; i++ {
			lensStats.Add(uint64(k))
		}
	}

	var n50 uint64
	var l50 int
	var q1, q2, q3 float64
	if *opts.All {
		n50 = lensStats.N50()
		l50 = lensStats.L50()
		q1, q2, q3 = lensStats.Q1(), lensStats.Q2(), lensStats.Q3()
	}

	info := &StatInfo{name, format, t,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0,
		0, 0}

	if lensStats.Count() > 0 {
		info = &StatInfo{name, format, t,
			lensStats.Count(), lensStats.Sum(), gapSum, lensStats.Min(),
			math.Round(lensStats.Mean(), 1), lensStats.Max(), n50, l50,
			q1, q2, q3,
			math.Round(float64(q20)/float64(lensStats.Sum())*100, 2), math.Round(float64(q30)/float64(lensStats.Sum())*100, 2)}
	}

	_ = stats

	return info, nil
}

func StatsString(name string, format string, input *api.IDataFrame[string], o *SeqKitStatsOptions) (string, error) {
	info, err := Stats(name, format, input, o)
	if err != nil {
		return "", err
	}
	if o == nil {
		o = &SeqKitStatsOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	result := ""

	if *opts.Tabular {
		colnames := []string{
			"file",
			"format",
			"type",
			"num_seqs",
			"sum_len",
			"min_len",
			"avg_len",
			"max_len",
		}
		if *opts.All {
			colnames = append(colnames, []string{"Q1", "Q2", "Q3", "sum_gap", "N50", "Q20(%)", "Q30(%)"}...)
		}

		result += strings.Join(colnames, "\t") + "\n"

		if !*opts.All {
			result += fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%.1f\t%d\n",
				info.file,
				info.format,
				info.t,
				info.num,
				info.lenSum,
				info.lenMin,
				info.lenAvg,
				info.lenMax)
		} else {
			result += fmt.Sprintf("%s\t%s\t%s\t%d\t%d\t%d\t%.1f\t%d\t%.1f\t%.1f\t%.1f\t%d\t%d\t%.2f\t%.2f\n",
				info.file,
				info.format,
				info.t,
				info.num,
				info.lenSum,
				info.lenMin,
				info.lenAvg,
				info.lenMax,
				info.Q1,
				info.Q2,
				info.Q3,
				info.gapSum,
				info.N50,
				info.q20,
				info.q30)
		}
	} else {
		columns := []prettytable.Column{
			{Header: "file"},
			{Header: "format"},
			{Header: "type"},
			{Header: "num_seqs", AlignRight: true},
			{Header: "sum_len", AlignRight: true},
			{Header: "min_len", AlignRight: true},
			{Header: "avg_len", AlignRight: true},
			{Header: "max_len", AlignRight: true}}

		if *opts.All {
			columns = append(columns, []prettytable.Column{
				{Header: "Q1", AlignRight: true},
				{Header: "Q2", AlignRight: true},
				{Header: "Q3", AlignRight: true},
				{Header: "sum_gap", AlignRight: true},
				{Header: "N50", AlignRight: true},
				{Header: "Q20(%)", AlignRight: true},
				{Header: "Q30(%)", AlignRight: true},
				// {Header: "L50", AlignRight: true},
			}...)
		}

		tbl, err := prettytable.NewTable(columns...)
		if err != nil {
			return "", err
		}

		if !*opts.All {
			tbl.AddRow(
				info.file,
				info.format,
				info.t,
				humanize.Comma(int64(info.num)),
				humanize.Comma(int64(info.lenSum)),
				humanize.Comma(int64(info.lenMin)),
				humanize.Commaf(info.lenAvg),
				humanize.Comma(int64(info.lenMax)))
		} else {
			tbl.AddRow(
				info.file,
				info.format,
				info.t,
				humanize.Comma(int64(info.num)),
				humanize.Comma(int64(info.lenSum)),
				humanize.Comma(int64(info.lenMin)),
				humanize.Commaf(info.lenAvg),
				humanize.Comma(int64(info.lenMax)),
				humanize.Commaf(info.Q1),
				humanize.Commaf(info.Q2),
				humanize.Commaf(info.Q3),
				humanize.Comma(int64(info.gapSum)),
				humanize.Comma(int64(info.N50)),
				humanize.Commaf(info.q20),
				humanize.Commaf(info.q30),
				// humanize.Comma(info.L50),
			)
		}
		result += tbl.String()
	}
	return result, nil
}

type StatInfo struct {
	file   string
	format string
	t      string

	num    uint64
	lenSum uint64
	gapSum uint64
	lenMin uint64

	lenAvg float64
	lenMax uint64
	N50    uint64
	L50    int

	Q1 float64
	Q2 float64
	Q3 float64

	q20 float64
	q30 float64
}
