package com.bluegosling.redis.values;

/**
 * The type of a bit field, indicating signed vs. unsigned and the number of bits.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public final class BitFieldType {
   private final boolean signed;
   private final int numBits;
   
   /**
    * Creates a bit field type representing the given number of bits, signed.
    * 
    * @param numBits the number of bits
    * @return a signed bit field type
    */
   public static BitFieldType signed(int numBits) {
      return new BitFieldType(true, numBits);
   }

   /**
    * Creates a bit field type representing the given number of bits, unsigned.
    * 
    * @param numBits the number of bits
    * @return an unsigned bit field type
    */
   public static BitFieldType unsigned(int numBits) {
      return new BitFieldType(false, numBits);
   }
   
   BitFieldType(boolean signed, int numBits) {
      this.signed = signed;
      this.numBits = numBits;
   }
   
   @Override
   public boolean equals(Object o) {
      if (o instanceof BitFieldType) {
         BitFieldType other = (BitFieldType) o;
         return signed == other.signed && numBits == other.numBits;
      }
      return false;
   }
   
   @Override
   public int hashCode() {
      return signed ? numBits : ~numBits;
   }
   
   @Override
   public String toString() {
      return (signed ? "i" : "u") + numBits;
   }
}