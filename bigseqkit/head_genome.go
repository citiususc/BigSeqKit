package bigseqkit

import (
	"github.com/shenwei356/bio/seqio/fastx"
	"github.com/shenwei356/util/stringutil"
	"ignis/driver/api"
	"strings"
)

type SeqKitHeadGenomeOptions struct {
	inner HeadGenomeOptions
}

type HeadGenomeOptions struct {
	Config          KitConfig
	MiniCommonWords *int64
}

func (this *HeadGenomeOptions) setDefaults() *HeadGenomeOptions {
	this.Config.setDefaults()
	setDefault(&this.MiniCommonWords, 1)

	return this
}

func (this *SeqKitHeadGenomeOptions) Config(v *SeqKitConfig) *SeqKitHeadGenomeOptions {
	this.inner.Config = v.inner
	return this
}

func (this *SeqKitHeadGenomeOptions) MiniCommonWords(v int64) *SeqKitHeadGenomeOptions {
	this.inner.MiniCommonWords = &v
	return this
}

func HeadGenome(input *api.IDataFrame[string], o *SeqKitHeadGenomeOptions) (*api.IDataFrame[string], error) {
	if o == nil {
		o = &SeqKitHeadGenomeOptions{}
	}
	opts := o.inner
	opts.setDefaults()

	firstSeq, err := input.Take(1)
	if err != nil {
		return nil, err
	}
	fastxReader, err := fastx.NewReaderFromIO(nil, strings.NewReader(firstSeq[0]), *opts.Config.IDRegexp)
	if err != nil {
		return nil, err
	}
	record, err := fastxReader.Read()
	if err != nil {
		return nil, err
	}

	prefixes := stringutil.Split(string(record.Desc), "\t ")

	lib, err := api.AddParam(libSource("HeadGenome"), "opts", OptionsToString(opts))
	if err != nil {
		return nil, err
	}
	lib, err = api.AddParam(lib, "prefixes", prefixes)
	if err != nil {
		return nil, err
	}

	return api.MapPartitionsWithIndex[string, string](input, lib)
}
