package datawriter_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/documatrix/datawriter"
	"github.com/stretchr/testify/require"
)

func create(t *testing.T) (*datawriter.Writer, *bytes.Buffer) {
	byteBuf := []byte{}
	buf := bytes.NewBuffer(byteBuf)
	w := datawriter.NewWriter(buf)

	return w, buf
}

func TestWriterString(t *testing.T) {
	w, buf := create(t)

	err := w.Write("hallo", "\"", "\\\n\r\t\x08\x1A")
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)
	require.Equal(t, "\"hallo\",\"\\\"\",\"\\\\\\n\\r\\t\\b\\Z\"\n", buf.String())
}

func TestWriterInt(t *testing.T) {
	w, buf := create(t)

	err := w.Write(int(1), int8(2), int16(3), int32(4), int64(5))
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)
	require.Equal(t, "\"1\",\"2\",\"3\",\"4\",\"5\"\n", buf.String())
}

func TestWriterUint(t *testing.T) {
	w, buf := create(t)

	err := w.Write(uint(1), uint8(2), uint16(3), uint32(4), uint64(5))
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)
	require.Equal(t, "\"1\",\"2\",\"3\",\"4\",\"5\"\n", buf.String())
}

func TestWriterBool(t *testing.T) {
	w, buf := create(t)

	err := w.Write(true, false)
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)
	require.Equal(t, "\"1\",\"0\"\n", buf.String())
}

func TestWriteByteArray(t *testing.T) {
	w, buf := create(t)

	data := []byte{
		0x00,
		0x01,
		0x02,
		0x03,
		0x04,
		0x05,
		0x06,
		0x07,
		0x08,
		0x09,
		0x0A,
		0x0B,
		0x0C,
		0x0D,
		0x0E,
		0x0F,
		0x10,
		0x11,
		0x12,
		0x13,
		0x14,
		0x15,
		0x16,
		0x17,
		0x18,
		0x19,
		0x1A,
		0x1B,
		0x1C,
		0x1D,
		0x1E,
		0x1F,
	}

	err := w.Write(data)
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)
	require.Equal(t, "\"\\0\x01\x02\x03\x04\x05\x06\x07\\b\\t\\n\x0b\x0c\\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\\Z\x1B\x1C\x1D\x1E\x1F\"\n", buf.String())
}

func TestWriteFloat(t *testing.T) {
	w, buf := create(t)

	err := w.Write(float32(1.0), float64(2.5))
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)
	require.Equal(t, "\"1\",\"2.5\"\n", buf.String())
}

func TestWriteTime(t *testing.T) {
	w, buf := create(t)

	loc, err := time.LoadLocation("UTC")
	require.Nil(t, err)
	err = w.Write(time.Date(
		2018,
		5,
		30,
		12,
		1,
		2,
		0,
		loc,
	), time.Time{})
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)
	require.Equal(t, "\"2018-05-30 12:01:02\",\"0000-00-00 00:00:00\"\n", buf.String())
}

func TestWriteInvalidType(t *testing.T) {
	w, _ := create(t)

	err := w.Write(struct{ A int }{1})
	require.NotNil(t, err)
}

func TestNil(t *testing.T) {
	w, buf := create(t)

	err := w.Write(nil)
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)
	require.Equal(t, "\\N\n", buf.String())
}
