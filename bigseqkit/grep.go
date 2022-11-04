package bigseqkit

import (
	"ignis/driver/api"
	"ignis/executor/api/ipair"
	"strconv"
)

type SeqKitGrepOptions struct {
	inner GrepOptions
}

type GrepOptions struct {
	Config             KitConfig
	Pattern            *[]string
	PatternFile        *string
	UseRegexp          *bool
	DeleteMatched      *bool
	InvertMatch        *bool
	ByName             *bool
	BySeq              *bool
	OnlyPositiveStrand *bool
	MaxMismatch        *int
	IgnoreCase         *bool
	Degenerate         *bool
	Region             *string
	Circular           *bool
	Count              *bool
}

func (this *GrepOptions) setDefaults() *GrepOptions {
	this.Config.setDefaults()
	setDefault(&this.Pattern, []string{""})
	setDefault(&this.PatternFile, "")
	setDefault(&this.UseRegexp, false)
	setDefault(&this.DeleteMatched, false)
	setDefault(&this.InvertMatch, false)
	setDefault(&this.ByName, false)
	setDefault(&this.BySeq, false)
	setDefault(&this.OnlyPositiveStrand, false)
	setDefault(&this.MaxMismatch, 0)
	setDefault(&this.IgnoreCase, false)
	setDefault(&this.Degenerate, false)
	setDefault(&this.Region, "")
	setDefault(&this.Circular, false)
	setDefault(&this.Count, false)

	return this
}

func (this *SeqKitGrepOptions) Config(v *SeqKitConfig) *SeqKitGrepOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitGrepOptions) Pattern(v []string) *SeqKitGrepOptions {
	this.inner.Pattern = &v
	return this
}

func (this *SeqKitGrepOptions) PatternFile(v string) *SeqKitGrepOptions {
	this.inner.PatternFile = &v
	return this
}

func (this *SeqKitGrepOptions) UseRegexp(v bool) *SeqKitGrepOptions {
	this.inner.UseRegexp = &v
	return this
}

func (this *SeqKitGrepOptions) DeleteMatched(v bool) *SeqKitGrepOptions {
	this.inner.DeleteMatched = &v
	return this
}

func (this *SeqKitGrepOptions) InvertMatch(v bool) *SeqKitGrepOptions {
	this.inner.InvertMatch = &v
	return this
}

func (this *SeqKitGrepOptions) ByName(v bool) *SeqKitGrepOptions {
	this.inner.ByName = &v
	return this
}

func (this *SeqKitGrepOptions) BySeq(v bool) *SeqKitGrepOptions {
	this.inner.BySeq = &v
	return this
}

func (this *SeqKitGrepOptions) OnlyPositiveStrand(v bool) *SeqKitGrepOptions {
	this.inner.OnlyPositiveStrand = &v
	return this
}

func (this *SeqKitGrepOptions) MaxMismatch(v int) *SeqKitGrepOptions {
	this.inner.MaxMismatch = &v
	return this
}

func (this *SeqKitGrepOptions) IgnoreCase(v bool) *SeqKitGrepOptions {
	this.inner.IgnoreCase = &v
	return this
}

func (this *SeqKitGrepOptions) Degenerate(v bool) *SeqKitGrepOptions {
	this.inner.Degenerate = &v
	return this
}

func (this *SeqKitGrepOptions) Region(v string) *SeqKitGrepOptions {
	this.inner.Region = &v
	return this
}

func (this *SeqKitGrepOptions) Circular(v bool) *SeqKitGrepOptions {
	this.inner.Circular = &v
	return this
}

func commonGrep(input *api.IDataFrame[string], opts *GrepOptions) (*api.IDataFrame[string], error) {
	grep, err := api.AddParam(libSource("GrepPairMatched"), "opts", OptionsToString(*opts))
	if err != nil {
		return nil, err
	}

	return api.MapPartitionsWithIndex[string, string](input, grep)
}

func Grep(input *api.IDataFrame[string], o *SeqKitGrepOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitGrepOptions{}
	}
	opts := o.inner
	opts.setDefaults()
	aux := false
	opts.Count = &aux

	results, err := commonGrep(input, &opts)
	if err != nil {
		return nil, err
	}

	if *opts.BySeq && *opts.MaxMismatch > 0 {
		return results, nil
	} else if *opts.DeleteMatched && !*opts.InvertMatch {
		f1, err := api.Map[string, ipair.IPair[string, string]](results, libSource("GrepPairMatched"))
		if err != nil {
			return nil, err
		}
		f2, err := api.ToPair[string, string](f1).ReduceByKey(libSource("GrepReducePairMatched"), false)
		if err != nil {
			return nil, err
		}
		return api.Map[ipair.IPair[string, string], string](f2.FromPair(), libSource("GrepValueMatched"))
	} else {
		return results, nil
	}
}

func GrepCount(input *api.IDataFrame[string], o *SeqKitGrepOptions) (int64, error) {
	if o == nil {
		o = &SeqKitGrepOptions{}
	}
	opts := o.inner
	opts.setDefaults()
	aux := true
	opts.Count = &aux

	results, err := commonGrep(input, &opts)
	if err != nil {
		return 0, err
	}

	count, err := results.Reduce(libSource("GrepReduceCount"))
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(count, 10, 64)
}
