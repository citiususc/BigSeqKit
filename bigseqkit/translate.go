package bigseqkit

import "ignis/driver/api"

type SeqKitTranslateOptions struct {
	inner TranslateOptions
}

type TranslateOptions struct {
	Config                       KitConfig
	TranslTable                  *int
	Frame                        *[]string
	Trim                         *bool
	Clean                        *bool
	AllowUnknownCodon            *bool
	InitCodonAsM                 *bool
	ListTranslTable              *int
	ListTranslTableWithAmbCodons *int
	AppendFrame                  *bool
}

func (this *TranslateOptions) setDefaults() *TranslateOptions {
	this.Config.setDefaults()
	setDefault(&this.TranslTable, 1)
	setDefault(&this.Frame, []string{"1"})
	setDefault(&this.Trim, false)
	setDefault(&this.Clean, false)
	setDefault(&this.AllowUnknownCodon, false)
	setDefault(&this.InitCodonAsM, false)
	setDefault(&this.ListTranslTable, -1)
	setDefault(&this.ListTranslTableWithAmbCodons, -1)
	setDefault(&this.AppendFrame, false)

	return this
}

func (this *SeqKitTranslateOptions) Config(v *SeqKitConfig) *SeqKitTranslateOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitTranslateOptions) TranslTable(v int) *SeqKitTranslateOptions {
	this.inner.TranslTable = &v
	return this
}

func (this *SeqKitTranslateOptions) Frame(v []string) *SeqKitTranslateOptions {
	this.inner.Frame = &v
	return this
}

func (this *SeqKitTranslateOptions) Trim(v bool) *SeqKitTranslateOptions {
	this.inner.Trim = &v
	return this
}

func (this *SeqKitTranslateOptions) Clean(v bool) *SeqKitTranslateOptions {
	this.inner.Clean = &v
	return this
}

func (this *SeqKitTranslateOptions) AllowUnknownCodon(v bool) *SeqKitTranslateOptions {
	this.inner.AllowUnknownCodon = &v
	return this
}

func (this *SeqKitTranslateOptions) InitCodonAsM(v bool) *SeqKitTranslateOptions {
	this.inner.InitCodonAsM = &v
	return this
}

func (this *SeqKitTranslateOptions) ListTranslTable(v int) *SeqKitTranslateOptions {
	this.inner.ListTranslTable = &v
	return this
}

func (this *SeqKitTranslateOptions) ListTranslTableWithAmbCodons(v int) *SeqKitTranslateOptions {
	this.inner.ListTranslTableWithAmbCodons = &v
	return this
}

func (this *SeqKitTranslateOptions) AppendFrame(v bool) *SeqKitTranslateOptions {
	this.inner.AppendFrame = &v
	return this
}

func Translate(input *api.IDataFrame[string], o *SeqKitTranslateOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitTranslateOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	libprepare, err := api.AddParam(libSource("Translate"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}

	return api.MapPartitions[string, string](input, libprepare)
}
