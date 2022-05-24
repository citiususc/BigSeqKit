package seqkit

import "ignis/driver/api"

type SeqKitReplaceOptions struct {
	inner ReplaceOptions
}

type ReplaceOptions struct {
	Config      KitConfig
	Pattern     *string
	Replacement *string
	NrWidth     *int
	BySeq       *bool
	IgnoreCase  *bool
	KvFile      *string
	KeepUntouch *bool
	KeepKey     *bool
	KeyCaptIdx  *int
	KeyMissRepl *string
}

func (this *ReplaceOptions) setDefaults() *ReplaceOptions {
	this.Config.setDefaults()
	setDefault(&this.Pattern, "")
	setDefault(&this.Replacement, "")
	setDefault(&this.NrWidth, 1)
	setDefault(&this.BySeq, false)
	setDefault(&this.IgnoreCase, false)
	setDefault(&this.KvFile, "")
	setDefault(&this.KeepUntouch, false)
	setDefault(&this.KeepKey, false)
	setDefault(&this.KeyCaptIdx, 1)
	setDefault(&this.KeyMissRepl, "")

	return this
}

func (this *SeqKitReplaceOptions) Config(v *SeqKitConfig) *SeqKitReplaceOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitReplaceOptions) Pattern(v string) *SeqKitReplaceOptions {
	this.inner.Pattern = &v
	return this
}

func (this *SeqKitReplaceOptions) Replacement(v string) *SeqKitReplaceOptions {
	this.inner.Replacement = &v
	return this
}

func (this *SeqKitReplaceOptions) NrWidth(v int) *SeqKitReplaceOptions {
	this.inner.NrWidth = &v
	return this
}

func (this *SeqKitReplaceOptions) BySeq(v bool) *SeqKitReplaceOptions {
	this.inner.BySeq = &v
	return this
}

func (this *SeqKitReplaceOptions) IgnoreCase(v bool) *SeqKitReplaceOptions {
	this.inner.IgnoreCase = &v
	return this
}

func (this *SeqKitReplaceOptions) KvFile(v string) *SeqKitReplaceOptions {
	this.inner.KvFile = &v
	return this
}

func (this *SeqKitReplaceOptions) KeepUntouch(v bool) *SeqKitReplaceOptions {
	this.inner.KeepUntouch = &v
	return this
}

func (this *SeqKitReplaceOptions) KeepKey(v bool) *SeqKitReplaceOptions {
	this.inner.KeepKey = &v
	return this
}

func (this *SeqKitReplaceOptions) KeyCaptIdx(v int) *SeqKitReplaceOptions {
	this.inner.KeyCaptIdx = &v
	return this
}

func (this *SeqKitReplaceOptions) KeyMissRepl(v string) *SeqKitReplaceOptions {
	this.inner.KeyMissRepl = &v
	return this
}

func Replace(input *api.IDataFrame[string], o *SeqKitReplaceOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitReplaceOptions{}
	}
	opts := o.inner
	opts.setDefaults()
	libprepare, err := api.AddParam(libSource("Replace"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	return api.MapPartitions[string, string](input, libprepare)
}
