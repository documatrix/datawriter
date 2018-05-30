package datawriter

import (
	"bufio"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

// Writer can be used to write some data in a CSV file, which can be loaded
// by MySQL using LOAD DATA INFILE
type Writer struct {
	w       *bufio.Writer
	Delim   rune
	Quote   rune
	LineEnd string
}

// NewWriter returns a new writer, which will write to the given io.Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:       bufio.NewWriter(w),
		Delim:   ',',
		Quote:   '"',
		LineEnd: "\n",
	}
}

// Write will write the record with the given fields
func (w *Writer) Write(fields ...interface{}) error {
	for n, field := range fields {
		if n > 0 {
			if _, err := w.w.WriteRune(w.Delim); err != nil {
				return err
			}
		}

		if field == nil {
			w.w.WriteString("\\N")
			continue
		}

		v := reflect.ValueOf(field)

		k := v.Kind()
		if k == reflect.Ptr && v.IsNil() {
			w.w.WriteString("\\N")
		} else {
			if k == reflect.Ptr {
				v = reflect.Indirect(v)
				field = v.Interface()
				k = v.Kind()
			}
			var err error
			_, err = w.w.WriteRune(w.Quote)
			if err != nil {
				return fmt.Errorf("Error while writing opening quote! %s", err)
			}

			if k == reflect.String {
				s := field.(string)
				for i := 0; i < len(s); i++ {
					err = w.writeByte(s[i])
					if err != nil {
						return fmt.Errorf("Error while writing %s to output file! %s", s, err)
					}
				}

			} else if k == reflect.Bool {
				if field.(bool) {
					_, err = w.w.WriteString("1")
				} else {
					_, err = w.w.WriteString("0")
				}
			} else if k == reflect.Int {
				_, err = w.w.WriteString(strconv.Itoa(field.(int)))
			} else if k == reflect.Int8 {
				_, err = w.w.WriteString(strconv.Itoa(int(field.(int8))))
			} else if k == reflect.Int16 {
				_, err = w.w.WriteString(strconv.Itoa(int(field.(int16))))
			} else if k == reflect.Int32 {
				_, err = w.w.WriteString(strconv.Itoa(int(field.(int32))))
			} else if k == reflect.Int64 {
				_, err = w.w.WriteString(strconv.FormatInt(field.(int64), 10))
			} else if k == reflect.Uint {
				_, err = w.w.WriteString(strconv.FormatUint(uint64(field.(uint)), 10))
			} else if k == reflect.Uint8 {
				_, err = w.w.WriteString(strconv.FormatUint(uint64(field.(uint8)), 10))
			} else if k == reflect.Uint16 {
				_, err = w.w.WriteString(strconv.FormatUint(uint64(field.(uint16)), 10))
			} else if k == reflect.Uint32 {
				_, err = w.w.WriteString(strconv.FormatUint(uint64(field.(uint32)), 10))
			} else if k == reflect.Uint64 {
				_, err = w.w.WriteString(strconv.FormatUint(field.(uint64), 10))
			} else if k == reflect.Float32 {
				_, err = w.w.WriteString(strconv.FormatFloat(float64(field.(float32)), 'f', -1, 64))
			} else if k == reflect.Float64 {
				_, err = w.w.WriteString(strconv.FormatFloat(field.(float64), 'f', -1, 64))
			} else if k == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
				bytes := field.([]byte)
				for i := 0; i < len(bytes); i++ {
					err = w.writeByte(bytes[i])
					if err != nil {
						return fmt.Errorf("Error while writing %+v to output file! %s", bytes, err)
					}
				}

			} else {
				return fmt.Errorf("Error printing data %+v! It has an unsupported type %s", field, k.String())
			}

			if err != nil {
				return fmt.Errorf("Error while writing value %+v! %s", field, err)
			}

			_, err = w.w.WriteRune(w.Quote)
			if err != nil {
				return fmt.Errorf("Error while writing closing quote! %s", err)
			}
		}
	}

	w.w.WriteString(w.LineEnd)

	return nil
}

// Flush will call Flush on the underlying writer.
func (w *Writer) Flush() error {
	return w.w.Flush()
}

func (w *Writer) writeByte(b byte) error {
	var err error
	switch b {
	case '\x00':
		_, err = w.w.WriteString("\\0")
	case '\\':
		_, err = w.w.WriteString("\\b")
	case '\x0a':
		_, err = w.w.WriteString("\\n")
	case '\x0d':
		_, err = w.w.WriteString("\\r")
	case '\x09':
		_, err = w.w.WriteString("\\t")
	case '\x1a':
		_, err = w.w.WriteString("\\Z")
	default:
		err = w.w.WriteByte(b)
	}

	return err
}
