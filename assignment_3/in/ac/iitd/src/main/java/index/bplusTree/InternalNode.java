package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

   @Override
    public T[] getKeys() {
        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        int offset = 6;

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


    @Override
    public void insert(T key, int right_block_id) {
        if(right_block_id == -1){
            return;
        }
        /* Write your code here */
        int numKeys = getNumKeys();
        int offset = 6 ;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            byte[] existingKeyBytes = this.get_data(offset + 2, keyLength);
            T existingKey = convertBytesToT(existingKeyBytes, this.typeClass);
            int cmp = compare(existingKey,key,this.typeClass);
            if (cmp > 0) {
                break;
            }
            offset += ( keyLength + 4);
        }

        byte[] new_data = convertTToBytes(key,this.typeClass);
        int shift_length = new_data.length+4;

        byte[] nextFreeOffset= this.get_data(2,2);
        int nextoffset = (nextFreeOffset[0] << 8) | (nextFreeOffset[1] & 0xFF);
        if (nextoffset != offset) {
            byte[] src = this.get_data(offset, nextoffset - offset);
            this.write_data(offset + shift_length, src);
        }

        
        byte[] newDataLengthBytes = new byte[2];
        newDataLengthBytes[0] = (byte) (new_data.length >> 8);
        newDataLengthBytes[1] = (byte) new_data.length;
        this.write_data(offset, newDataLengthBytes);
        this.write_data(offset + 2, new_data);

        byte[] blockIdBytes = new byte[2];
        blockIdBytes[0] = (byte) (right_block_id >> 8);
        blockIdBytes[1] = (byte) right_block_id;
        this.write_data(offset+new_data.length + 2 , blockIdBytes);
       
        int numEntries = getNumKeys() + 1;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) (numEntries >> 8);
        numEntriesBytes[1] = (byte) numEntries;
        this.write_data(0, numEntriesBytes);

        int newOffset = nextoffset+shift_length;
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (newOffset >> 8);
        nextFreeOffsetBytes[1] = (byte) newOffset;
        this.write_data(2, nextFreeOffsetBytes);
        
        return;
    }


    public byte[] splitNode() {
        int numKeys = getNumKeys();
        int offset =  6;
        for (int i = 0; i < numKeys/2; i++) {
            byte[] keyLengthBytes = this.get_data(offset, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            offset += (2 + keyLength + 2);
        }
        byte[] nextFreeOffset= this.get_data(2,2);
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
        this.write_data(2, nextFreeOffsetBytes);
        return data;
    }

    @Override
    public int search(T key) {
        int numKeys = getNumKeys();
        int offset = 6;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            byte[] keyBytes = this.get_data(offset + 2, keyLength);
            T nodeKey = convertBytesToT(keyBytes,this.typeClass);

            int cmp = compare(nodeKey,key,this.typeClass);
            if (cmp > 0) {
                byte[] childBytes = this.get_data(offset - 2, 2);
                return (childBytes[0] << 8) | (childBytes[1] & 0xFF);
            }
            if(cmp == 0){
                byte[] childBytes = this.get_data(offset+keyLength + 2, 2);
                return (childBytes[0] << 8) | (childBytes[1] & 0xFF);
            }
            offset += (2 + keyLength + 2);
        }

        byte[] childBytes = this.get_data(offset - 2, 2);
        return (childBytes[0] << 8) | (childBytes[1] & 0xFF);
    }

    public int search1(T key) {
        int numKeys = getNumKeys();
        int offset = 6;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);

            byte[] keyBytes = this.get_data(offset + 2, keyLength);
            T nodeKey = convertBytesToT(keyBytes,this.typeClass);

            int cmp = compare(nodeKey,key,this.typeClass);
            if (cmp > 0) {
                byte[] childBytes = this.get_data(offset - 2, 2);
                return (childBytes[0] << 8) | (childBytes[1] & 0xFF);
            }
            offset += (2 + keyLength + 2);
        }

        byte[] childBytes = this.get_data(offset - 2, 2);
        int a= (childBytes[0] << 8) | (childBytes[1] & 0xFF);
        return (childBytes[0] << 8) | (childBytes[1] & 0xFF);
    }

    public void insert2(byte[] data, int keys){
        int length1= data.length;
        this.write_data(6,data);

        int numEntries = keys;
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = (byte) (numEntries >> 8);
        numEntriesBytes[1] = (byte) numEntries;
        this.write_data(0, numEntriesBytes);

        int newOffset = 6+length1;
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = (byte) (newOffset >> 8);
        nextFreeOffsetBytes[1] = (byte) newOffset;
        this.write_data(2, nextFreeOffsetBytes);
        return;
    }



    public int[] getChildren() {
        int numKeys = getNumKeys();
        int[] children = new int[numKeys + 1];

        /* Write your code here */
        int offset = 6;
        for (int i = 0; i < numKeys; i++) {
            byte[] childBytes = this.get_data(offset - 2, 2);
            children[i]= (childBytes[0] << 8) | (childBytes[1] & 0xFF);
            byte[] keyLengthBytes = this.get_data(offset, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);

            offset += (2 + keyLength + 2);
        }
        byte[] childBytes = this.get_data(offset - 2, 2);
        children[numKeys]= (childBytes[0] << 8) | (childBytes[1] & 0xFF);

        return children;

    }

}
