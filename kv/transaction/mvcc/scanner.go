package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn      *MvccTxn
	iter     engine_util.DBIterator
	indexKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{
		iter: iter,
		txn:  txn,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for scan.iter.Valid() {
		key := DecodeUserKey(scan.iter.Item().Key())
		startKey := EncodeKey(key, scan.txn.StartTS)
		endKey := EncodeKey(key, 0)
		if !engine_util.ExceedEndKey(scan.iter.Item().Key(), startKey) {
			scan.iter.Seek(startKey)
		}

		if !engine_util.ExceedEndKey(scan.iter.Item().Key(), endKey) {
			writeData, err := scan.iter.Item().ValueCopy(nil)
			if err != nil {
				return nil, nil, err
			}
			scan.iter.Seek(endKey)

			write, err := ParseWrite(writeData)
			if err != nil {
				return nil, nil, err
			}
			if write.Kind == WriteKindDelete {
				continue
			}
			value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
			return key, value, err
		}
	}

	return nil, nil, nil
}
