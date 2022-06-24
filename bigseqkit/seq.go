package bigseqkit

import "ignis/driver/api"

type SeqKitSeqOptions struct {
	inner SeqOptions
}

type SeqOptions struct {
	Config            KitConfig
	Reverse           *bool
	Complement        *bool
	Name              *bool
	Seq               *bool
	Qual              *bool
	OnlyId            *bool
	RemoveGaps        *bool
	GapLetters        *string
	LowerCase         *bool
	UpperCase         *bool
	Dna2rna           *bool
	Rna2dna           *bool
	ValidateSeq       *bool
	ValidateSeqLength *int
	MaxLen            *int
	MinLen            *int
	QualAsciiBase     *int
	MinQual           *float64
	MaxQual           *float64
}

func (this *SeqOptions) setDefaults() *SeqOptions {
	this.Config.setDefaults()
	setDefault(&this.Reverse, false)
	setDefault(&this.Complement, false)
	setDefault(&this.Name, false)
	setDefault(&this.Seq, false)
	setDefault(&this.Qual, false)
	setDefault(&this.OnlyId, false)
	setDefault(&this.RemoveGaps, false)
	setDefault(&this.GapLetters, "- 	.")
	setDefault(&this.LowerCase, false)
	setDefault(&this.UpperCase, false)
	setDefault(&this.Dna2rna, false)
	setDefault(&this.Rna2dna, false)
	setDefault(&this.ValidateSeq, false)
	setDefault(&this.ValidateSeqLength, 10000)
	setDefault(&this.MaxLen, -1)
	setDefault(&this.MinLen, -1)
	setDefault(&this.QualAsciiBase, 33)
	setDefault(&this.MinQual, -1)
	setDefault(&this.MaxQual, -1)

	return this
}

func (this *SeqKitSeqOptions) Config(v *SeqKitConfig) *SeqKitSeqOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitSeqOptions) Reverse(v bool) *SeqKitSeqOptions {
	this.inner.Reverse = &v
	return this
}

func (this *SeqKitSeqOptions) Complement(v bool) *SeqKitSeqOptions {
	this.inner.Complement = &v
	return this
}

func (this *SeqKitSeqOptions) Name(v bool) *SeqKitSeqOptions {
	this.inner.Name = &v
	return this
}

func (this *SeqKitSeqOptions) Seq(v bool) *SeqKitSeqOptions {
	this.inner.Seq = &v
	return this
}

func (this *SeqKitSeqOptions) Qual(v bool) *SeqKitSeqOptions {
	this.inner.Qual = &v
	return this
}

func (this *SeqKitSeqOptions) OnlyId(v bool) *SeqKitSeqOptions {
	this.inner.OnlyId = &v
	return this
}

func (this *SeqKitSeqOptions) RemoveGaps(v bool) *SeqKitSeqOptions {
	this.inner.RemoveGaps = &v
	return this
}

func (this *SeqKitSeqOptions) GapLetters(v string) *SeqKitSeqOptions {
	this.inner.GapLetters = &v
	return this
}

func (this *SeqKitSeqOptions) LowerCase(v bool) *SeqKitSeqOptions {
	this.inner.LowerCase = &v
	return this
}

func (this *SeqKitSeqOptions) UpperCase(v bool) *SeqKitSeqOptions {
	this.inner.UpperCase = &v
	return this
}

func (this *SeqKitSeqOptions) Dna2rna(v bool) *SeqKitSeqOptions {
	this.inner.Dna2rna = &v
	return this
}

func (this *SeqKitSeqOptions) Rna2dna(v bool) *SeqKitSeqOptions {
	this.inner.Rna2dna = &v
	return this
}

func (this *SeqKitSeqOptions) ValidateSeq(v bool) *SeqKitSeqOptions {
	this.inner.ValidateSeq = &v
	return this
}

func (this *SeqKitSeqOptions) ValidateSeqLength(v int) *SeqKitSeqOptions {
	this.inner.ValidateSeqLength = &v
	return this
}

func (this *SeqKitSeqOptions) MaxLen(v int) *SeqKitSeqOptions {
	this.inner.MaxLen = &v
	return this
}

func (this *SeqKitSeqOptions) MinLen(v int) *SeqKitSeqOptions {
	this.inner.MinLen = &v
	return this
}

func (this *SeqKitSeqOptions) QualAsciiBase(v int) *SeqKitSeqOptions {
	this.inner.QualAsciiBase = &v
	return this
}

func (this *SeqKitSeqOptions) MinQual(v float64) *SeqKitSeqOptions {
	this.inner.MinQual = &v
	return this
}

func (this *SeqKitSeqOptions) MaxQual(v float64) *SeqKitSeqOptions {
	this.inner.MaxQual = &v
	return this
}

func Seq(input *api.IDataFrame[string], o *SeqKitSeqOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitSeqOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	libprepare, err := api.AddParam(libSource("SeqTransform"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	return api.MapPartitions[string, string](input, libprepare)
}
