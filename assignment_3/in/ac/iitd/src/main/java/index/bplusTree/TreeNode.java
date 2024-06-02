package index.bplusTree;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key); 
    // returns the block id where the key is present
    // for InternalNode, it will return the block id of the child node
    // for LeafNode, it will return the block id of the record


    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        if (typeClass.equals(String.class)) {
            return typeClass.cast(new String(bytes));
        } else if (typeClass.equals(Integer.class)) {
            return typeClass.cast(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt());
        } else if (typeClass.equals(Boolean.class)) {
            return typeClass.cast(bytes[0] != 0);
        } else if (typeClass.equals(Float.class)) {
            return typeClass.cast(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat());
        } else if (typeClass.equals(Double.class)) {
            return typeClass.cast(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble());
        } else {
            return null;
        }
    }

    default public byte[] convertTToBytes(T value, Class<T> typeClass) {
        if (typeClass.equals(String.class)) {
            return ((String) value).getBytes();
        } else if (typeClass.equals(Integer.class)) {
            return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt((Integer) value).array();
        } else if (typeClass.equals(Boolean.class)) {
            return new byte[] { (byte) (((Boolean) value) ? 1 : 0) };
        } else if (typeClass.equals(Float.class)) {
            return ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat((Float) value).array();
        } else if (typeClass.equals(Double.class)) {
            return ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble((Double) value).array();
        } else {
            return null;
        }
    }


    default public int compare(T value1, T value2, Class<T> typeClass) {
        if (typeClass.equals(Integer.class)) {
            Integer int1 = (Integer) value1;
            Integer int2 = (Integer) value2;
            return int1.compareTo(int2);
        } else if (typeClass.equals(Boolean.class)) {
            Boolean bool1 = (Boolean) value1;
            Boolean bool2 = (Boolean) value2;
            return Boolean.compare(bool1, bool2);
        } else if (typeClass.equals(Float.class)) {
            Float float1 = (Float) value1;
            Float float2 = (Float) value2;
            return Float.compare(float1, float2);
        } else if (typeClass.equals(Double.class)) {
            Double double1 = (Double) value1;
            Double double2 = (Double) value2;
            return Double.compare(double1, double2);
        } else if (typeClass.equals(String.class)) {
            String str1 = (String) value1;
            String str2 = (String) value2;
            return str1.compareTo(str2);
        } else {
            throw new IllegalArgumentException("Unsupported data type");
        }
    }

    
}

