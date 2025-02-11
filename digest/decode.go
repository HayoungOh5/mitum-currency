package digest

import (
	mongodbstorage "github.com/ProtoconNet/mitum-currency/v3/digest/mongodb"
	"github.com/ProtoconNet/mitum2/base"
	"github.com/ProtoconNet/mitum2/util"
	"github.com/ProtoconNet/mitum2/util/encoder"
	"github.com/ProtoconNet/mitum2/util/valuehash"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
)

func LoadOperationHash(decoder func(interface{}) error) (util.Hash, error) {
	var doc struct {
		FH valuehash.Bytes `bson:"fact"`
	}

	if err := decoder(&doc); err != nil {
		return nil, err
	}
	return doc.FH, nil
}

func LoadOperation(decoder func(interface{}) error, encs *encoder.Encoders) (OperationValue, error) {
	var b bson.Raw
	if err := decoder(&b); err != nil {
		return OperationValue{}, err
	}

	if _, hinter, err := mongodbstorage.LoadDataFromDoc(b, encs); err != nil {
		return OperationValue{}, err
	} else if va, ok := hinter.(OperationValue); !ok {
		return OperationValue{}, errors.Errorf("not OperationValue: %T", hinter)
	} else {
		return va, nil
	}
}

func LoadAccountValue(decoder func(interface{}) error, encs *encoder.Encoders) (AccountValue, error) {
	var b bson.Raw
	if err := decoder(&b); err != nil {
		return AccountValue{}, err
	}

	_, hinter, err := mongodbstorage.LoadDataFromDoc(b, encs)
	if err != nil {
		return AccountValue{}, err
	}

	rs, ok := hinter.(AccountValue)
	if !ok {
		return AccountValue{}, errors.Errorf("not AccountValue: %T", hinter)
	}

	return rs, nil
}

func LoadBalance(decoder func(interface{}) error, encs *encoder.Encoders) (base.State, error) {
	var b bson.Raw
	if err := decoder(&b); err != nil {
		return nil, err
	}

	if _, hinter, err := mongodbstorage.LoadDataFromDoc(b, encs); err != nil {
		return nil, err
	} else if st, ok := hinter.(base.State); !ok {
		return nil, errors.Errorf("not base.State: %T", hinter)
	} else {
		return st, nil
	}
}

func LoadCurrency(decoder func(interface{}) error, encs *encoder.Encoders) (base.State, error) {
	var b bson.Raw

	if err := decoder(&b); err != nil {
		return nil, err
	}

	if _, hinter, err := mongodbstorage.LoadDataFromDoc(b, encs); err != nil {
		return nil, err
	} else if st, ok := hinter.(base.State); !ok {
		return nil, errors.Errorf("not base.State: %T", hinter)
	} else {
		return st, nil
	}
}

func LoadContractAccountStatus(decoder func(interface{}) error, encs *encoder.Encoders) (base.State, error) {
	var b bson.Raw

	if err := decoder(&b); err != nil {
		return nil, err
	}

	if _, hinter, err := mongodbstorage.LoadDataFromDoc(b, encs); err != nil {
		return nil, err
	} else if st, ok := hinter.(base.State); !ok {
		return nil, errors.Errorf("not base.State: %T", hinter)
	} else {
		return st, nil
	}
}

func LoadManifest(decoder func(interface{}) error, encs *encoder.Encoders) (base.Manifest, uint64, error) {
	var b bson.Raw

	if err := decoder(&b); err != nil {
		return nil, 0, err
	}

	if _, hinter, operations, err := mongodbstorage.LoadManifestDataFromDoc(b, encs); err != nil {
		return nil, 0, err
	} else if m, ok := hinter.(base.Manifest); !ok {
		return nil, 0, errors.Errorf("not base.Manifest: %T", hinter)
	} else {
		return m, operations, nil
	}
}

func LoadState(decoder func(interface{}) error, encs *encoder.Encoders) (base.State, error) {
	var b bson.Raw

	if err := decoder(&b); err != nil {
		return nil, err
	}

	if _, hinter, err := mongodbstorage.LoadDataFromDoc(b, encs); err != nil {
		return nil, err
	} else if st, ok := hinter.(base.State); !ok {
		return nil, errors.Errorf("not base.State: %T", hinter)
	} else {
		return st, nil
	}
}
