package bigseqkit

import "ignis/driver/api"

type SeqKitSubseqOptions struct {
	inner SubseqOptions
}

type SubseqOptions struct {
	Config     KitConfig
	Chr        *[]string
	Region     *string
	Gtf        *string
	Feature    *[]string
	UpStream   *int
	DownStream *int
	OnlyFlank  *bool
	Bed        *string
	GtfTag     *string
}

func (this *SubseqOptions) setDefaults() *SubseqOptions {
	this.Config.setDefaults()
	setDefault(&this.Chr, []string{})
	setDefault(&this.Region, "")
	setDefault(&this.Gtf, "")
	setDefault(&this.Feature, []string{})
	setDefault(&this.UpStream, 0)
	setDefault(&this.DownStream, 0)
	setDefault(&this.OnlyFlank, false)
	setDefault(&this.Bed, "")
	setDefault(&this.GtfTag, "")

	return this
}

func (this *SeqKitSubseqOptions) Config(v *SeqKitConfig) *SeqKitSubseqOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitSubseqOptions) Chr(v []string) *SeqKitSubseqOptions {
	this.inner.Chr = &v
	return this
}

func (this *SeqKitSubseqOptions) Region(v string) *SeqKitSubseqOptions {
	this.inner.Region = &v
	return this
}

func (this *SeqKitSubseqOptions) Gtf(v string) *SeqKitSubseqOptions {
	this.inner.Gtf = &v
	return this
}

func (this *SeqKitSubseqOptions) Feature(v []string) *SeqKitSubseqOptions {
	this.inner.Feature = &v
	return this
}

func (this *SeqKitSubseqOptions) UpStream(v int) *SeqKitSubseqOptions {
	this.inner.UpStream = &v
	return this
}

func (this *SeqKitSubseqOptions) DownStream(v int) *SeqKitSubseqOptions {
	this.inner.DownStream = &v
	return this
}

func (this *SeqKitSubseqOptions) OnlyFlank(v bool) *SeqKitSubseqOptions {
	this.inner.OnlyFlank = &v
	return this
}

func (this *SeqKitSubseqOptions) Bed(v string) *SeqKitSubseqOptions {
	this.inner.Bed = &v
	return this
}

func (this *SeqKitSubseqOptions) GtfTag(v string) *SeqKitSubseqOptions {
	this.inner.GtfTag = &v
	return this
}

func Subseq(input *api.IDataFrame[string], o *SeqKitSubseqOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitSubseqOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	libprepare, err := api.AddParam(libSource("SubseqTransform"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	return api.MapPartitions[string, string](input, libprepare)
}
