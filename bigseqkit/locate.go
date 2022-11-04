package bigseqkit

import "ignis/driver/api"

type SeqKitLocateOptions struct {
	inner LocateOptions
}

type LocateOptions struct {
	Config             KitConfig
	Pattern            *[]string
	PatternFile        *string
	Degenerate         *bool
	UseRegexp          *bool
	UseFmi             *bool
	IgnoreCase         *bool
	OnlyPositiveStrand *bool
	ValidateSeqLength  *int
	NonGreedy          *bool
	Gtf                *bool
	Bed                *bool
	MaxMismatch        *int
	HideMatched        *bool
	Circular           *bool
}

func (this *LocateOptions) setDefaults() *LocateOptions {
	this.Config.setDefaults()
	setDefault(&this.Pattern, []string{""})
	setDefault(&this.PatternFile, "")
	setDefault(&this.Degenerate, false)
	setDefault(&this.UseRegexp, false)
	setDefault(&this.UseFmi, false)
	setDefault(&this.IgnoreCase, false)
	setDefault(&this.OnlyPositiveStrand, false)
	setDefault(&this.ValidateSeqLength, 10000)
	setDefault(&this.NonGreedy, false)
	setDefault(&this.Gtf, false)
	setDefault(&this.Bed, false)
	setDefault(&this.MaxMismatch, 0)
	setDefault(&this.HideMatched, false)
	setDefault(&this.Circular, false)

	return this
}

func (this *SeqKitLocateOptions) Config(v *SeqKitConfig) *SeqKitLocateOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitLocateOptions) Pattern(v []string) *SeqKitLocateOptions {
	this.inner.Pattern = &v
	return this
}

func (this *SeqKitLocateOptions) PatternFile(v string) *SeqKitLocateOptions {
	this.inner.PatternFile = &v
	return this
}

func (this *SeqKitLocateOptions) Degenerate(v bool) *SeqKitLocateOptions {
	this.inner.Degenerate = &v
	return this
}

func (this *SeqKitLocateOptions) UseRegexp(v bool) *SeqKitLocateOptions {
	this.inner.UseRegexp = &v
	return this
}

func (this *SeqKitLocateOptions) UseFmi(v bool) *SeqKitLocateOptions {
	this.inner.UseFmi = &v
	return this
}

func (this *SeqKitLocateOptions) IgnoreCase(v bool) *SeqKitLocateOptions {
	this.inner.IgnoreCase = &v
	return this
}

func (this *SeqKitLocateOptions) OnlyPositiveStrand(v bool) *SeqKitLocateOptions {
	this.inner.OnlyPositiveStrand = &v
	return this
}

func (this *SeqKitLocateOptions) ValidateSeqLength(v int) *SeqKitLocateOptions {
	this.inner.ValidateSeqLength = &v
	return this
}

func (this *SeqKitLocateOptions) NonGreedy(v bool) *SeqKitLocateOptions {
	this.inner.NonGreedy = &v
	return this
}

func (this *SeqKitLocateOptions) Gtf(v bool) *SeqKitLocateOptions {
	this.inner.Gtf = &v
	return this
}

func (this *SeqKitLocateOptions) Bed(v bool) *SeqKitLocateOptions {
	this.inner.Bed = &v
	return this
}

func (this *SeqKitLocateOptions) MaxMismatch(v int) *SeqKitLocateOptions {
	this.inner.MaxMismatch = &v
	return this
}

func (this *SeqKitLocateOptions) HideMatched(v bool) *SeqKitLocateOptions {
	this.inner.HideMatched = &v
	return this
}

func (this *SeqKitLocateOptions) Circular(v bool) *SeqKitLocateOptions {
	this.inner.Circular = &v
	return this
}

func Locate(input *api.IDataFrame[string], o *SeqKitLocateOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitLocateOptions{}
	}
	opts := o.inner
	opts.setDefaults()
	libprepare, err := api.AddParam(libSource("Locate"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	return api.MapPartitionsWithIndex[string, string](input, libprepare)
}
