package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TupWritable implements Writable, Iterable<Writable> {
   private long written;
   private Writable[] values;

   public TupWritable() {
   }

   public TupWritable(Writable[] vals) {
      this.written = 0L;
      this.values = vals;
      this.setWritten(0);
      this.setWritten(1);
   }

   public boolean has(int i) {
      return 0L != (1L << i & this.written);
   }

   public Writable get(int i) {
      return this.values[i];
   }

   public int size() {
      return this.values.length;
   }

   public boolean equals(Object other) {
      if (other instanceof TupWritable) {
         TupWritable that = (TupWritable) other;
         if (this.size() == that.size() && this.written == that.written) {
            for (int i = 0; i < this.values.length; ++i) {
               if (this.has(i) && !this.values[i].equals(that.get(i))) {
                  return false;
               }
            }
            return true;
         }
      }
      return false;
   }

   public int hashCode() {
      assert false : "hashCode not designed";
      return (int) this.written;
   }

   public Iterator<Writable> iterator() {
      return new Iterator<Writable>() {
         private int index = 0;

         @Override
         public boolean hasNext() {
            while (index < values.length && !has(index)) {
               index++;
            }
            return index < values.length;
         }

         @Override
         public Writable next() {
            return values[index++];
         }
      };
   }

   public String toString() {
      StringBuffer buf = new StringBuffer("[");
      for (int i = 0; i < this.values.length; ++i) {
         buf.append(this.has(i) ? this.values[i].toString() : "");
         buf.append(",");
      }
      if (this.values.length != 0) {
         buf.setCharAt(buf.length() - 1, ']');
      } else {
         buf.append(']');
      }
      return buf.toString();
   }

   public void write(DataOutput out) throws IOException {
      WritableUtils.writeVInt(out, this.values.length);
      WritableUtils.writeVLong(out, this.written);
      for (int i = 0; i < this.values.length; ++i) {
         Text.writeString(out, this.values[i].getClass().getName());
      }
      for (int i = 0; i < this.values.length; ++i) {
         if (this.has(i)) {
            this.values[i].write(out);
         }
      }
   }

   public void readFields(DataInput in) throws IOException {
      int card = WritableUtils.readVInt(in);
      this.values = new Writable[card];
      this.written = WritableUtils.readVLong(in);
      Class<? extends Writable>[] cls = new Class[card];

      try {
         for (int i = 0; i < card; ++i) {
            cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
         }
         for (int i = 0; i < card; ++i) {
            this.values[i] = (Writable) cls[i].newInstance();
            if (this.has(i)) {
               this.values[i].readFields(in);
            }
         }
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
         throw new IOException("Failed tuple init", e);
      }
   }

   void setWritten(int i) {
      this.written |= 1L << i;
   }

   void clearWritten(int i) {
      this.written &= ~(1L << i);
   }

   void clearWritten() {
      this.written = 0L;
   }
}