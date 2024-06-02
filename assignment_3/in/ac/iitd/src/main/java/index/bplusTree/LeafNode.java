package index.bplusTree;

/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */

public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */

        int offset = 10;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            byte[] keyBytes = this.get_data(offset + 2, keyLength);
            T nodeKey = convertBytesToT(keyBytes, this.typeClass);
            keys[i] = nodeKey;
            offset += (2 + keyLength + 2); 
        }
        return keys;
    }

    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */
        int offset = 10;
        for (int i = 0; i < numKeys; i++) {
            byte[] childBytes = this.get_data(offset - 2, 2);
            block_ids[i]= (childBytes[0] << 8) | (childBytes[1] & 0xFF);
            byte[] keyLengthBytes = this.get_data(offset, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            offset += (2 + keyLength + 2);
        }
        return block_ids;

    }

    @Override
    public void insert(T key, int block_id) {
        int numKeys = getNumKeys();
        int offset = 8;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset+2, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            byte[] existingKeyBytes = this.get_data(offset + 4, keyLength);
            T existingKey = convertBytesToT(existingKeyBytes, this.typeClass);
            int cmp = compare(existingKey,key,this.typeClass);
            if (cmp > 0) {
                break;
            }
            offset += ( keyLength + 4);
        }

        byte[] new_data = convertTToBytes(key,this.typeClass);
        int shift_length = new_data.length+4;

        byte[] nextFreeOffset= this.get_data(6,2);
        int nextoffset = (nextFreeOffset[0] << 8) | (nextFreeOffset[1] & 0xFF);
        if (nextoffset != offset) {
            byte[] src = this.get_data(offset, nextoffset - offset);
            this.write_data(offset + shift_length, src);
        }

        byte[] blockIdBytes = new byte[2];
        blockIdBytes[0] = (byte) (block_id >> 8);
        blockIdBytes[1] = (byte) block_id;
        this.write_data(offset, blockIdBytes);

        byte[] newDataLengthBytes = new byte[2];
        newDataLengthBytes[0] = (byte) (new_data.length >> 8);
        newDataLengthBytes[1] = (byte) new_data.length;
        this.write_data(offset + 2, newDataLengthBytes);

        this.write_data(offset + 4, new_data);

        int numEntries = getNumKeys() + 1;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) (numEntries >> 8);
        numEntriesBytes[1] = (byte) numEntries;
        this.write_data(0, numEntriesBytes);

        int newOffset = nextoffset+shift_length;
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (newOffset >> 8);
        nextFreeOffsetBytes[1] = (byte) newOffset;
        this.write_data(6, nextFreeOffsetBytes);
        
        return;
    }



    @Override
    public int search(T key) {

        /* Write your code here */
        int numKeys = getNumKeys();
        int offset = 10;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            byte[] keyBytes = this.get_data(offset + 2, keyLength);
            T nodeKey = convertBytesToT(keyBytes,this.typeClass);

            int cmp = compare(nodeKey,key,this.typeClass);
            if (cmp == 0) {
                byte[] childBytes = this.get_data(offset - 2, 2);
                return (childBytes[0] << 8) | (childBytes[1] & 0xFF);
            } 
            offset += (2 + keyLength + 2);
        }

        return -1;
    }

    public byte[] splitleaf() {

        /* Write your code here */
        int numKeys = getNumKeys();
        int offset =  8;
        for (int i = 0; i < (numKeys)/2; i++) {
            byte[] keyLengthBytes = this.get_data(offset+2, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            offset += (2 + keyLength + 2);
        }
        byte[] nextFreeOffset= this.get_data(6,2);
        int nextoffset = (nextFreeOffset[0] << 8) | (nextFreeOffset[1] & 0xFF);

        byte[] data= this.get_data(offset,nextoffset-offset);

        int numEntries = (numKeys)/2;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) (numEntries >> 8);
        numEntriesBytes[1] = (byte) numEntries;
        this.write_data(0, numEntriesBytes);

        int newOffset = offset;
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (newOffset >> 8);
        nextFreeOffsetBytes[1] = (byte) newOffset;
        this.write_data(6, nextFreeOffsetBytes);
        
        return data;
    }

    public void insert1(byte[] data, int keys, int prev_pointer, byte[] next_pointer){
        int length1= data.length;
        this.write_data(8,data);
        int numEntries = keys;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) (numEntries >> 8);
        numEntriesBytes[1] = (byte) numEntries;
        this.write_data(0, numEntriesBytes);

        byte[] prevBytes = new byte[2];
        prevBytes[0] = (byte) (prev_pointer >> 8);
        prevBytes[1] = (byte) prev_pointer;
        this.write_data(2, prevBytes);
        this.write_data(4,next_pointer);

        int newOffset = 8+length1;
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (newOffset >> 8);
        nextFreeOffsetBytes[1] = (byte) newOffset;
        this.write_data(6, nextFreeOffsetBytes);
        return;
    }

    
}
