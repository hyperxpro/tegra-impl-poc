package org.tegra.pds.art;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * ART inner node with up to 4 children, using sorted keys for binary search.
 * Path compression is supported via prefix storage.
 *
 * @param <V> the value type
 */
public final class Node4<V> implements ArtNode<V> {

    private final byte[] keys;
    @SuppressWarnings("unchecked")
    private final ArtNode<V>[] children;
    private final int count;
    private final byte[] prefix;
    private final int prefixLen;

    @SuppressWarnings("unchecked")
    Node4(byte[] keys, ArtNode<V>[] children, int count, byte[] prefix, int prefixLen) {
        this.keys = keys;
        this.children = children;
        this.count = count;
        this.prefix = prefix;
        this.prefixLen = prefixLen;
    }

    @SuppressWarnings("unchecked")
    static <V> Node4<V> empty(byte[] prefix, int prefixLen) {
        return new Node4<>(new byte[4], new ArtNode[4], 0, prefix, prefixLen);
    }

    /**
     * Adds a child at the given key byte, maintaining sorted order.
     * Returns a new Node4 (used during construction).
     */
    @SuppressWarnings("unchecked")
    Node4<V> addChild(byte keyByte, ArtNode<V> child) {
        // Find insertion point (sorted)
        int pos = 0;
        int kb = keyByte & 0xFF;
        while (pos < count && (keys[pos] & 0xFF) < kb) {
            pos++;
        }

        byte[] newKeys = keys.clone();
        ArtNode<V>[] newChildren = children.clone();

        // Shift right
        for (int i = count; i > pos; i--) {
            newKeys[i] = newKeys[i - 1];
            newChildren[i] = newChildren[i - 1];
        }
        newKeys[pos] = keyByte;
        newChildren[pos] = child;

        return new Node4<>(newKeys, newChildren, count + 1, prefix, prefixLen);
    }

    private int findChild(byte keyByte) {
        int kb = keyByte & 0xFF;
        for (int i = 0; i < count; i++) {
            if ((keys[i] & 0xFF) == kb) return i;
        }
        return -1;
    }

    private int checkPrefix(byte[] key, int depth) {
        int matched = 0;
        while (matched < prefixLen && depth + matched < key.length) {
            if (prefix[matched] != key[depth + matched]) {
                return matched;
            }
            matched++;
        }
        return matched;
    }

    @Override
    public ArtNode<V> put(byte[] key, int depth, V value) {
        // Check prefix
        int matched = checkPrefix(key, depth);
        if (matched < prefixLen) {
            // Prefix mismatch — need to split this node
            return splitPrefix(key, depth, value, matched);
        }

        int newDepth = depth + prefixLen;
        if (newDepth >= key.length) {
            // Key exhausted at this node — this shouldn't happen in normal usage
            // but handle gracefully
            return this;
        }

        byte keyByte = key[newDepth];
        int childIdx = findChild(keyByte);

        if (childIdx >= 0) {
            // Child exists — recurse
            ArtNode<V> child = children[childIdx];
            ArtNode<V> newChild = child.put(key, newDepth + 1, value);
            if (newChild == child) return this;

            ArtNode<V>[] newChildren = children.clone();
            newChildren[childIdx] = newChild;
            return new Node4<>(keys.clone(), newChildren, count, prefix, prefixLen);
        }

        // Need to add new child
        if (count < 4) {
            return addChild(keyByte, new Leaf<>(key, value));
        }

        // Upgrade to Node16
        return growToNode16().put(key, depth, value);
    }

    @SuppressWarnings("unchecked")
    private ArtNode<V> splitPrefix(byte[] key, int depth, V value, int matched) {
        // Create a new node with the shared prefix portion
        byte[] newPrefix = new byte[matched];
        System.arraycopy(prefix, 0, newPrefix, 0, matched);

        Node4<V> newNode = Node4.empty(newPrefix, matched);

        // Create a child for the existing subtree with remaining prefix
        byte existingByte = prefix[matched];
        int remainingLen = prefixLen - matched - 1;
        byte[] remainingPrefix = new byte[Math.max(remainingLen, 0)];
        if (remainingLen > 0) {
            System.arraycopy(prefix, matched + 1, remainingPrefix, 0, remainingLen);
        }
        Node4<V> existingChild = new Node4<>(keys.clone(), children.clone(), count, remainingPrefix, remainingLen);
        newNode = newNode.addChild(existingByte, existingChild);

        // Create a child (leaf) for the new key
        int newDepth = depth + matched;
        if (newDepth < key.length) {
            byte newByte = key[newDepth];
            newNode = newNode.addChild(newByte, new Leaf<>(key, value));
        }

        return newNode;
    }

    @SuppressWarnings("unchecked")
    private Node16<V> growToNode16() {
        byte[] newKeys = new byte[16];
        ArtNode<V>[] newChildren = new ArtNode[16];
        System.arraycopy(keys, 0, newKeys, 0, count);
        System.arraycopy(children, 0, newChildren, 0, count);
        return new Node16<>(newKeys, newChildren, count, prefix, prefixLen);
    }

    @Override
    public ArtNode<V> remove(byte[] key, int depth) {
        int matched = checkPrefix(key, depth);
        if (matched < prefixLen) {
            return this; // prefix mismatch, key not found
        }

        int newDepth = depth + prefixLen;
        if (newDepth >= key.length) {
            return this;
        }

        byte keyByte = key[newDepth];
        int childIdx = findChild(keyByte);
        if (childIdx < 0) {
            return this; // child not found
        }

        ArtNode<V> child = children[childIdx];
        ArtNode<V> newChild = child.remove(key, newDepth + 1);

        if (newChild == child) {
            return this; // nothing changed
        }

        if (newChild == null) {
            // Remove child
            if (count == 2) {
                // Collapse: only one child remains
                int remainingIdx = (childIdx == 0) ? 1 : 0;
                ArtNode<V> remaining = children[remainingIdx];
                byte remainingKey = keys[remainingIdx];

                // If remaining child is a leaf, return it directly (prefix doesn't matter for leaves)
                if (remaining instanceof Leaf) {
                    return remaining;
                }

                // Merge prefix: this.prefix + remainingKey + child.prefix
                if (remaining instanceof Node4<V> n4) {
                    return mergeWithChild(remainingKey, n4.prefix, n4.prefixLen, n4);
                } else if (remaining instanceof Node16<V> n16) {
                    return mergeWithChild(remainingKey, n16.prefix(), n16.prefixLen(), remaining);
                } else if (remaining instanceof Node48<V> n48) {
                    return mergeWithChild(remainingKey, n48.prefix(), n48.prefixLen(), remaining);
                } else if (remaining instanceof Node256<V> n256) {
                    return mergeWithChild(remainingKey, n256.prefix(), n256.prefixLen(), remaining);
                }
                return remaining;
            }

            // Remove child from this node
            return removeChild(childIdx);
        }

        // Replace child
        ArtNode<V>[] newChildren = children.clone();
        newChildren[childIdx] = newChild;
        return new Node4<>(keys.clone(), newChildren, count, prefix, prefixLen);
    }

    @SuppressWarnings("unchecked")
    private ArtNode<V> mergeWithChild(byte keyByte, byte[] childPrefix, int childPrefixLen, ArtNode<V> child) {
        int newPrefixLen = this.prefixLen + 1 + childPrefixLen;
        byte[] newPrefix = new byte[newPrefixLen];
        System.arraycopy(this.prefix, 0, newPrefix, 0, this.prefixLen);
        newPrefix[this.prefixLen] = keyByte;
        if (childPrefixLen > 0) {
            System.arraycopy(childPrefix, 0, newPrefix, this.prefixLen + 1, childPrefixLen);
        }

        // Rebuild the child node with the new merged prefix
        if (child instanceof Node4<V> n4) {
            return new Node4<>(n4.keys.clone(), n4.children.clone(), n4.count, newPrefix, newPrefixLen);
        } else if (child instanceof Node16<V> n16) {
            return n16.withPrefix(newPrefix, newPrefixLen);
        } else if (child instanceof Node48<V> n48) {
            return n48.withPrefix(newPrefix, newPrefixLen);
        } else if (child instanceof Node256<V> n256) {
            return n256.withPrefix(newPrefix, newPrefixLen);
        }
        return child;
    }

    @SuppressWarnings("unchecked")
    private Node4<V> removeChild(int idx) {
        byte[] newKeys = new byte[4];
        ArtNode<V>[] newChildren = new ArtNode[4];
        int j = 0;
        for (int i = 0; i < count; i++) {
            if (i != idx) {
                newKeys[j] = keys[i];
                newChildren[j] = children[i];
                j++;
            }
        }
        return new Node4<>(newKeys, newChildren, count - 1, prefix, prefixLen);
    }

    @Override
    public V get(byte[] key, int depth) {
        int matched = checkPrefix(key, depth);
        if (matched < prefixLen) {
            return null;
        }

        int newDepth = depth + prefixLen;
        if (newDepth >= key.length) {
            return null;
        }

        byte keyByte = key[newDepth];
        int childIdx = findChild(keyByte);
        if (childIdx < 0) {
            return null;
        }

        return children[childIdx].get(key, newDepth + 1);
    }

    @Override
    public int size() {
        int s = 0;
        for (int i = 0; i < count; i++) {
            s += children[i].size();
        }
        return s;
    }

    @Override
    public void forEach(BiConsumer<byte[], V> action) {
        for (int i = 0; i < count; i++) {
            children[i].forEach(action);
        }
    }

    // Accessors for sibling node types
    byte[] prefix() { return prefix; }
    int prefixLen() { return prefixLen; }
    int count() { return count; }
    byte[] keys() { return keys; }
    ArtNode<V>[] children() { return children; }

    Node4<V> withPrefix(byte[] newPrefix, int newPrefixLen) {
        return new Node4<>(keys.clone(), children.clone(), count, newPrefix, newPrefixLen);
    }
}
