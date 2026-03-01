package index

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
)

type Bitmap struct {
	rb *roaring.Bitmap
}

func NewBitmap() *Bitmap {
	return &Bitmap{rb: roaring.New()}
}

func NewBitmapFrom(rb *roaring.Bitmap) *Bitmap {
	return &Bitmap{rb: rb}
}

func (b *Bitmap) Add(docID uint32) {
	b.rb.Add(docID)
}

func (b *Bitmap) Remove(docID uint32) {
	b.rb.Remove(docID)
}

func (b *Bitmap) Contains(docID uint32) bool {
	return b.rb.Contains(docID)
}

func (b *Bitmap) And(other *Bitmap) *Bitmap {
	if other == nil || other.rb == nil {
		return NewBitmap()
	}
	return &Bitmap{rb: roaring.And(b.rb, other.rb)}
}

func (b *Bitmap) Or(other *Bitmap) *Bitmap {
	if other == nil || other.rb == nil {
		return b.Clone()
	}
	return &Bitmap{rb: roaring.Or(b.rb, other.rb)}
}

func (b *Bitmap) AndNot(other *Bitmap) *Bitmap {
	if other == nil || other.rb == nil {
		return b.Clone()
	}
	return &Bitmap{rb: roaring.AndNot(b.rb, other.rb)}
}

func (b *Bitmap) Not(universe *Bitmap) *Bitmap {
	if universe == nil || universe.rb == nil {
		return NewBitmap()
	}
	return &Bitmap{rb: roaring.AndNot(universe.rb, b.rb)}
}

func (b *Bitmap) Clone() *Bitmap {
	return &Bitmap{rb: b.rb.Clone()}
}

func (b *Bitmap) Cardinality() uint64 {
	return b.rb.GetCardinality()
}

func (b *Bitmap) IsEmpty() bool {
	return b.rb.IsEmpty()
}

func (b *Bitmap) ToArray() []uint32 {
	return b.rb.ToArray()
}

func (b *Bitmap) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	_, err := b.rb.WriteTo(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Deserialize(data []byte) (*Bitmap, error) {
	rb := roaring.New()
	_, err := rb.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return &Bitmap{rb: rb}, nil
}
