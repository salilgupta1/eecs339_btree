#include <assert.h>
#include "btree.h"
#include <math.h>

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	if (op==BTREE_OP_LOOKUP) { 
	  return b.GetVal(offset,value);
	} else { 
	  rc= b.SetVal(offset,value);
	  if(rc){return rc;}
	  return b.Serialize(buffercache,node);
	}     
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


ERROR_T	BTreeIndex::InsertFindNode(const SIZE_T &Node, const KEY_T &key, const VALUE_T &value, list<SIZE_T> &Path) const
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,Node);
  Path.push_back(Node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return InsertFindNode(ptr,key,value,Path);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return InsertFindNode(ptr,key,value,Path);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
	return ERROR_NOERROR;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}

//=====HELPER FUNCTIONS FOR INSERTING============//
//
//
//
bool BTreeIndex::isFull(const SIZE_T &Node) const
{
	// Checks if a given node is full
	BTreeNode b; 
	b.Unserialize(buffercache, Node);
	switch(b.info.nodetype)
	{
		case BTREE_ROOT_NODE:
		case BTREE_INTERIOR_NODE:
		{
			return (b.info.GetNumSlotsAsInterior() == b.info.numkeys);
			break;
		}
		case BTREE_LEAF_NODE:
		{
			return (b.info.GetNumSlotsAsLeaf() == b.info.numkeys);
			break;
		}
		default:
		{
			// no such nodetype
			return false;
			break;
		}
	}
}


ERROR_T BTreeIndex::InsertAndSplitLeaf(SIZE_T &L1, SIZE_T &L2, const KEY_T &k, const VALUE_T &v){
	// distribute keys from one leaf to two keys
	BTreeNode original;
	BTreeNode newLeaf;
	
	ERROR_T rc;
	
	rc = original.Unserialize(buffercache,L1);
	if (rc){return rc};
	
	SIZE_T firstHalfOfKeys;
	SIZE_T secondHalfOfKeys;
	
	// find the index to split on
	firstHalfOfKeys = floor((original.info.numkeys + 1)/2);//3
	secondHalfOfKeys = ceil((original.info.numkeys + 1)/2);//4
	
	// read the new leaf from disk
	rc = newLeaf.Unserialize(buffercache, L2);
	// set the new leaf's num of keys
	newLeaf.info.numkeys = secondHalfOfKeys;
	
	if(rc){return rc;}
	
	KEY_T tempKey;
	SIZE_T offset;
	KEY_T newKey;
	VALUE_T newVal;
	
	// split the node into two leaves
	for (offset = firstHalfOfKeys; offset < original.info.numkeys;offset++)
	{
		// get from old leaf
		rc = original.GetKey(offset, newKey);
		rc = original.GetVal(offset, newVal);
		if(rc){return rc;}
		
		// put into new leaf
		rc = newLeaf.SetKey(offset-firstHalfOfKeys,newKey);
		rc = newLeaf.SetVal(offset-firstHalfOfKeys, newVal);
		if(rc){return rc;}
	}
	// set the original leaf's numkeys
	original.info.numkeys = firstHalfOfKeys;
	
	// now find where to put the new key and value
	rc = original.GetKey(firstHalfOfKeys - 1,tempKey);
	if(rc){return rc};
	
	// write the nodes back to disk
	rc = original.Serialize(buffercache, L1);
	if(rc){return rc;}
	rc = newLeaf.Serialize(buffercache,L2);
	if(rc){return rc;}
	
	if (k < tempKey || k == tempKey)
	{
		// we need to add our key to the first leaf
		rc = FindAndInsertKey(L1,k,v);
	}
	else
	{	
		// we need to add our key to the second leaf
		rc = FindAndInsertKey(L2,k,v);
	}
	if(rc){return rc;}
	
	return ERROR_NOERROR;
	
}

ERROR_T BTreeIndex::InsertRecur(list<SIZE_T> &path, SIZE_T &L2)
{
	SIZE_T p = path.pop_back();
	ERROR_T rc;
	// pull the parent from the path
	BTreeNode parent;
	// read it from disk
	rc = parent.Unserialize(buffercache, p);
	if(rc){return rc;}
	
	// read child from disk
	BTreeNode b;
	rc = b.Unserialize(buffercache, L2);
	if(rc){return rc;}
	
	// get the first key of the child
	KEY_T firstKey;
	SIZE_T ptr = &L2;
	rc = b.GetKey(0,firstKey);
	if(rc){return rc;}
	
	
	if(!isFull(p))
	{
		// if the parent isn't full 
		// then we put the first key into parent
		SIZE_T offset;
		KEY_T tempKey;
		SIZE_T saveOffset;
		SIZE_T tempPtr;
		// find where in parent to put the first key
		for(offset = 0; offset < parent.info.numkeys; offset++)
		{
			rc = parent.GetKey(offset, tempKey);
			if(rc){
				return rc;
			}
			if (firstKey<tempKey || firstKey==tempKey)
			{
				saveOffset = offset;
				break;
			}	
		}
		parent.info.numkeys++;
		// do the movement of keys/ptr to allocate space
		for(offset = parent.info.numkeys-1; offset > saveOffset; offset--)
		{
			rc = parent.GetKey(offset-1, tempKey);
			if(rc)
			{
				return rc;
			}
			rc = parent.GetPtr(offset-1, tempPtr);
			if(rc){return rc;}
			rc = parent.SetKey(offset,tempKey);
			rc = parent.SetPtr(offset,tempPtr);
			if(rc){return rc;}
		}
		rc = parent.SetKey(saveOffset, firstKey);
		rc = parent.SetPtr(saveoffset, ptr);
		if(rc){return rc;}
	}
	// if the parent is full and it is the root node 
	else if(parent.info.nodetype == BTREE_ROOT_NODE)
	{
		// we dont know 
	}
	// if the parent is full and it is an interior node
	else
	{
		SIZE_T newNode;
		// we need to create a new interior node
		rc = AllocateNode(newNode);
		if (rc){return rc;}
		// we need to take the parent and newNode and distribute the keys across the two
		InsertAndSplitInterior(p,newNode, firstKey, ptr)
	}
}

ERROR_T BTreeIndex::InsertAndSplitRoot(SIZE_T &L1, SIZE_T &L2){
        
}


ERROR_T BTreeIndex::InsertAndSplitInterior(SIZE_T &I1, SIZE_T &I2, const KEY_T &k, const SIZE_T &ptr)
{
        // Distribute keys and pointers of I1, plus new one, into I1 and I2 (except for middle)
        
}


ERROR_T BTreeIndex::FindAndInsertKey(SIZE_T &Node, const KEY_T &key, const VALUE_T &val)
{
	// find the place to insert a new key
	BTreeNode b;
	ERROR_T rc;
	rc = b.Unserialize(buffercache, Node);
	if (rc){return rc;}
	
	SIZE_T offset;
	KEY_T tempKey;
	SIZE_T saveOffset;
	KeyValuePair swapKV;
	VALUE_T tempVal;
	
	// find the place to put the key
	for(offset = 0; offset < b.info.numkeys; offset++)
	{
		rc = b.GetKey(offset, tempKey);
		if(rc){
			return rc;
		}
		if (key<tempKey || key==tempKey)
		{
			saveOffset = offset;
			break;
		}	
	}
	
	// move the keys down to allocate space for the new key
	for(offset = b.info.numkeys-1; offset > saveOffset; offset--)
	{
		rc = b.GetKey(offset-1, tempKey);
		if(rc)
		{
			return rc;
		}
		rc = b.GetVal(offset-1, tempVal);
		if (rc)
		{
			return rc;	
		}
		swapKV = KeyValuePair(tempKey, tempVal);
		rc = b.SetKeyVal(offset, swapKV);
       		if(rc)
       		{
        		return rc;
		}
		else
		{
			rc = b.SetKeyVal(offset, swapKV);
			if(rc)
			{
				return rc;
			}
		}
	}
	// put the new key in
	swapKV = KeyValuePair(key,val);
	rc = b.SetKeyVal(saveOffset, swapKV)
	if(rc){ return rc;}
	return ERROR_NOERROR;
	
}

ERROR_T BTreeIndex::InsertInternal(const SIZE_T &Node, const KEY_T &key, const VALUE_T &val)
{

	// Find leaf node L (capacity n) that new key would be inserted in
	ERROR_T rc;
	list<SIZE_T> Path;
	rc = InsertFindNode(Node, key, val, Path);
	
	SIZE_T L = Path.back();
	Path.pop_back();
	BTreeNode b;

	// If L is not full
	if(!isFull(L)){
		cout << "Not full!" << endl;		
		
		// get data from node
		rc = b.Unserialize(buffercache, L);

		SIZE_T offset;
		SIZE_T saveOffset = b.info.numkeys;
		KEY_T tempKey;
		const KeyValuePair kv = KeyValuePair(key, val);
		KeyValuePair swapKV;
		VALUE_T tempVal;

		// search for the location to put the key into the node
		for(offset = 0; offset<b.info.numkeys; offset++){
			rc = b.GetKey(offset, tempKey);
			if(rc){
				return rc;
			}
			if (key<tempKey || key==tempKey)
			{
				saveOffset = offset;
				break;
			}
		}

		cout << "Insert offset: " << saveOffset<<endl;
		cout << "Num keys before insert: " << b.info.numkeys << endl;

		// increment the number of keys in the node
		b.info.numkeys++;
		// go through the keys and move accordingly to allocate space for the new key
		for(offset = b.info.numkeys-1; offset > saveOffset; offset--)
		{
			cout << "In loop, offset is "<< offset <<endl;		
			rc = b.GetKey(offset-1, tempKey);
			if(rc)
			{
				return rc;
			}
			rc = b.GetVal(offset-1, tempVal);
			if (rc)
			{
				return rc;	
			}
			swapKV = KeyValuePair(tempKey, tempVal);
			
			// If it's the beginning where root is a leaf, insert as leaf
			if(b.info.nodetype == BTREE_ROOT_NODE  && superblock.info.freelist == 2){
				b.info.nodetype = BTREE_LEAF_NODE;
				rc = b.SetKeyVal(offset, swapKV);
               			if(rc)
               			{
                        		return rc;
                		}
				b.info.nodetype = BTREE_ROOT_NODE;
			}
			else
			{
				rc = b.SetKeyVal(offset, swapKV);
				if(rc)
				{
					return rc;
				}
			}
		}

		cout << "setting new keyval pair"<<endl;
		// insert our new key/val
		
		// If it's the beginning where root is a leaf, insert as leaf
		if(b.info.nodetype == BTREE_ROOT_NODE  && superblock.info.freelist == 2){
			b.info.nodetype = BTREE_LEAF_NODE;
			rc = b.SetKeyVal(saveOffset, kv);
               		if(rc)
               		{
                        	return rc;
                	}
			b.info.nodetype = BTREE_ROOT_NODE;
			
		}else{

			rc = b.SetKeyVal(saveOffset, kv);
			if(rc)
			{
				return rc;
			}
		}
		// write the data back to the disk
		return b.Serialize(buffercache, Node);
	}

	// if the node is full
	else{
		// read the data from the node
		rc = b.Unserialize(buffercache, L);
		if (rc){return rc};
		switch(b.info.nodetype)
		{
			case BTREE_ROOT_NODE:
			{
				// if the node we are looking at is a root node
				InsertAndSplitRoot();
				break;
			}
			default
			{
				// otherwise we must be looking at a leaf node
				SIZE_T L2;
				// allocate space for a new leaf node
				rc = AllocateNode(L2)
				if(rc){return rc;}
				// split the leaf and put half of keys into new leaf node
				rc = InsertAndSplitLeaf(L,L2,key,val);
				// go up the tree to its interior nodes and reshuffle things around
				rc = InsertRecur(Path, L2);
				break;	
			}
		}
	}
	// Else if L is full, aka has n keys already
	// 	1. Split L: Find a node L2 from the free list 
	//
	//	2. Divide the keys: <PASS L AND L2 INTO InsertAndSplitLeaf()>
	//		First ceil (n+1)/2 key-pointer pairs remain in L
	//		Last floor (n+1)/2 key-pointer pairs go into L2
	//
	//	3. Insert (first key of?) L2 into parent node P: <InsertRecur()>
	//		If there is space in node P
	//			Insert key-pointer pair to L2
	//			Done
	//		**NOTE**: IF ROOT HAS DIFFERENT STANDARDS FOR 'FULL', THIS NEEDS TO BE STRUCTURED DIFFERENTLY
	//		Else if there is no space and this is root: <InsertAndSplitRoot()>
	//			Do something special
	//			Done
	//		Else if there is no space and this is not root: 
	//			 Create new interior node P2 (same level as P)
	//			 
	//			 First ceil (n+2)/2 pointers stay in P
	//			 Last floor (n+2)/2 pointers move to P2
	//
	//			 First ceil (n/2) keys stay in P
	//			 Last floor (n/2) keys move to P2
	//			 K = leftover middle key indicating smallest key reachable via P2
	//
	//			 Repeat this step where parent is parent of P and L2 is K <PASS parent AND K INTO InsertRecur()>
	//
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  return InsertInternal(superblock.info.rootnode, key, (VALUE_T&) value);
}
  
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, (VALUE_T&) value);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  
  set<SIZE_T> checkedNodes;
  ERROR_T rc;
  SIZE_T rootNode = superblock.info.rootnode;
  rc = SanityCheckHelper(checkedNodes, rootNode);
  return rc;
}
  
ERROR_T BTreeIndex::SanityCheckHelper(set<SIZE_T> &checkedNodes, SIZE_T &node) const
{
	BTreeNode b;
	SIZE_T ptr;
	SIZE_T offset;
	ERROR_T rc;
	KEY_T testKey;
	KEY_T tempKey;


	// check to see if the node has already been checked
	// if it has then we have an inner loop which is wrong
	if (checkedNodes.count(node))
	{
		return ERROR_INNERLOOP;
	}
	else
	{
		// add to list of nodes we've checked
		checkedNodes.insert(node);
	}

	// unserialize the block
	rc = b.Unserialize(buffercache, node);
	if (rc)
	{
		return rc;
	}
	
	// root node, interior node and leaf node
	switch(b.info.nodetype)
	{
		case BTREE_INTERIOR_NODE:
		{
			// checks for overflow(i.e. fullness) in the interior node
			if((b.info.numkeys) >(int)(b.info.GetNumSlotsAsInterior()* (2./3.)))
			{
				return ERROR_NODEOVERFLOW;
			}
		}		
		case BTREE_ROOT_NODE:
		{
			// traverse to node's keys
			for (offset=0;offset<b.info.numkeys;offset++)
			{
				// get the key
				rc = b.GetKey(offset,testKey);
				if(rc)
				{
					return rc;
				}

				// get the next key
				if(offset+1<b.info.numkeys)
				{
					rc = b.GetKey(offset+1, tempKey);
					if(rc){return rc;}
					// check to make sure the keys are sorted
					if(tempKey < testKey)
					{
						return ERROR_BADORDER;
					}
				}
				// get the ptr to the next level
				rc = b.GetPtr(offset,ptr);
				if(rc)
				{
					return rc;
				}
				else
				{					
					// recurse to the next level 
					return SanityCheckHelper(checkedNodes, ptr);
				}						
			}
			if(b.info.numkeys > 0)
			{
				// get the very last ptr in the block 
				rc = b.GetPtr(b.info.numkeys,ptr);
				if (rc){ return rc;}
				// recurse to the next level
				return SanityCheckHelper(checkedNodes, ptr);
			}
			else
			{
				// no keys in the node 
				return ERROR_NONEXISTENT;
			}
			return ERROR_NOERROR;
			break;
		}
		case BTREE_LEAF_NODE:
		{
			if(b.info.numkeys > 0)
			{
				// check for fullness
				if(b.info.numkeys > (int)(b.info.GetNumSlotsAsLeaf()*(2./3.)))
				{
					return ERROR_NODEOVERFLOW;
				}
				else
				{
					// go through keys
					for(offset = 0; offset < b.info.numkeys; offset++)
					{
						rc = b.GetKey(offset,testKey);
						if (rc)
						{
							return rc;
						}
						// check for order
						if(offset+1 < b.info.numkeys)
						{
							rc = b.GetKey(offset+1, tempKey);
							if (rc) {return rc;}
							if (tempKey < testKey)
							{
								return ERROR_BADORDER;
							}
						}	
					}						
				}
			}
			else
			{
				// leaf node doesn't have keys
				return ERROR_NONEXISTENT;
			}

		return ERROR_NOERROR;
		break;
		}
		default:
		{
			// the block isn't a node we know about
			return ERROR_BADNODETYPE;
			break;
		}			
	}
	return ERROR_NOERROR;
	
}

ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_DEPTH_DOT);
  return os;
}



