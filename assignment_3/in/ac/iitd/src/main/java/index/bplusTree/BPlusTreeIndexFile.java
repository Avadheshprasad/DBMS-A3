package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node

    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

     private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }


    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    private int searchRecursive1(int nodeId, T key, ArrayList<Integer> visitedNodes) {
        BlockNode node = blocks.get(nodeId);
        visitedNodes.add(nodeId);

        if (isLeaf(nodeId)) {
            return nodeId;
        } else {
            InternalNode<T> internalNode = (InternalNode<T>) node;
            int childId = internalNode.search1(key);
            return searchRecursive1(childId, key,visitedNodes);
        }
    }

    private T convertBytesToT1(byte[] bytes, Class<T> typeClass) {
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

    




    public void insert(T key, int block_id) {
        int rootId = getRootId();
        BlockNode rootNode = blocks.get(rootId);

        ArrayList<Integer> visitedNodes = new ArrayList<>();
        int leafNodeId = searchRecursive1(rootId, key, visitedNodes);
        LeafNode<T> L = (LeafNode<T>) blocks.get(leafNodeId);
        if (!isFull(leafNodeId)) {
            L.insert(key, block_id);
        } else {
            L.insert(key, block_id);
            int numKeys = L.getNumKeys();
            byte[] insert_data= L.splitleaf();
            LeafNode<T> L1 = new LeafNode<>(this.typeClass);
            byte[] next_pointer=L.get_data(4,2);
            L1.insert1(insert_data,(numKeys+1)/2,leafNodeId,next_pointer);
            blocks.add(L1);
            int L1Id = blocks.size() - 1;

            byte[] bytesL1 = new byte[2];
            bytesL1[0] = (byte) (L1Id >> 8);
            bytesL1[1] = (byte) L1Id;

            L.write_data(4,bytesL1);

            byte[] keyLengthBytes = L1.get_data(10, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            byte[] keyBytes = L1.get_data(12, keyLength);
            T firstKeyOfL1 = convertBytesToT1(keyBytes, this.typeClass);
            visitedNodes.remove(visitedNodes.size() - 1);
            insertIntoParent(leafNodeId, firstKeyOfL1, L1Id, visitedNodes);
        }
    }


    private void insertIntoParent(int leftId, T key, int RightId,ArrayList<Integer> visitedNodes) {

        if (visitedNodes.isEmpty()) {
            InternalNode<T> root = new InternalNode<>(key,leftId, RightId, this.typeClass);
            blocks.add(root);
            BlockNode node = blocks.get(0);
            int rootId = blocks.size() - 1;
            byte[] bytesroot = new byte[2];
            bytesroot[0] = (byte) (rootId >> 8);
            bytesroot[1] = (byte) rootId;

            node.write_data(2,bytesroot);
            return;
        } else {
            int parentId = visitedNodes.get(visitedNodes.size() - 1);
            InternalNode<T> P = (InternalNode<T>) blocks.get(parentId);
            if (!isFull(parentId)) {
                P.insert(key,RightId);
                return;
            } else {
                P.insert(key,RightId);
                int numKeys = P.getNumKeys();
                byte[] insert_data= P.splitNode();

                int k1Length = (insert_data[0] << 8) | (insert_data[1] & 0xFF);
                byte[] K_1 = new byte[k1Length];
                for (int i = 0; i < k1Length; i++) {
                    K_1[i] = insert_data[i + 2];
                }
                T key1= convertBytesToT1(K_1,this.typeClass);
                int pointer = (insert_data[2 + k1Length] << 8) | (insert_data[3 + k1Length] & 0xFF);
                int rem_length=insert_data.length-4-k1Length;
                byte[] data_rem = new byte[rem_length];
                for (int i = 0; i+4+k1Length < insert_data.length; i++) {
                    data_rem[i] = insert_data[i+4+k1Length];
                }
                
                InternalNode<T> P1 = new InternalNode<T>(key1,pointer,-1,this.typeClass);
                P1.insert2(data_rem,(numKeys-1)/2);
                blocks.add(P1);
                int P1Id = blocks.size() - 1;
                
                visitedNodes.remove(visitedNodes.size() - 1);
                insertIntoParent(parentId, key1,P1Id, visitedNodes);
                return;
            }

        }

    }

    

    public int search(T key) {

        /* Write your code here */
            
        int rootId = getRootId();
        return searchRecursive(rootId, key);
    }


    private int searchRecursive(int nodeId, T key) {
        BlockNode node = blocks.get(nodeId);

        if (isLeaf(nodeId)) {
            return ((LeafNode<T>) node).search(key);
        } else {
            InternalNode<T> internalNode = (InternalNode<T>) node;
            int childId = internalNode.search(key);
            return searchRecursive(childId, key);
        }
    }


    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}