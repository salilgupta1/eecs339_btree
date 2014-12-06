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

  cout << "**Pushed onto path: "<<Node<<endl;
  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
	if(superblock.info.freelist == 2){
		return ERROR_NOERROR;
	}

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
			return true;
			break;
		}
	}
	return false;
}


ERROR_T BTreeIndex::InsertAndSplitLeaf(SIZE_T &L1, SIZE_T &L2, const KEY_T &k, const VALUE_T &v){
	// distribute keys from one leaf to two keys and then insert the new key/val pair
	BTreeNode original;
	BTreeNode newLeaf;
	
	ERROR_T rc;
	
	rc = original.Unserialize(buffercache,L1);
	if (rc){return rc;}
	
	SIZE_T firstHalfOfKeys;
	SIZE_T secondHalfOfKeys;
	
	// find the index to split on
	firstHalfOfKeys = floor((original.info.numkeys + 1)/2);
	secondHalfOfKeys = ceil((original.info.numkeys + 1)/2);
	
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
	if(rc){return rc;}
	
	// write the nodes back to disk
	rc = original.Serialize(buffercache, L1);
	if(rc){return rc;}
	rc = newLeaf.Serialize(buffercache,L2);
	if(rc){return rc;}
	
	if (k < tempKey || k == tempKey)
	{
		// we need to add our key to the first leaf
		rc = FindAndInsertKeyVal(L1,k,v);
	}
	else
	{	
		// we need to add our key to the second leaf
		rc = FindAndInsertKeyVal(L2,k,v);
	}
	if(rc){return rc;}
	
	return ERROR_NOERROR;
	
}

ERROR_T BTreeIndex::InsertRecur(list<SIZE_T> &path, const KEY_T &k , const SIZE_T &ptr)
{
	SIZE_T p = path.back();
	path.pop_back();
	ERROR_T rc;
	// pull the parent from the path
	BTreeNode parent;
	// read it from disk
	rc = parent.Unserialize(buffercache, p);
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
			if (k<tempKey || k==tempKey)
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
			rc = parent.GetPtr(offset, tempPtr);
			if(rc){return rc;}
			rc = parent.SetKey(offset,tempKey);
			rc = parent.SetPtr(offset + 1,tempPtr);
			if(rc){return rc;}
		}
		rc = parent.SetKey(saveOffset, k);
		rc = parent.SetPtr(saveOffset + 1, ptr);
		rc = parent.Serialize(buffercache,p);
		if(rc){return rc;}
	}
	// if the parent is full and it is the root node 
	else if(parent.info.nodetype == BTREE_ROOT_NODE)
	{
		SIZE_T NewNode;
		//we need to create a new interior node
		rc = AllocateNode(NewNode);
		
		SIZE_T NewRoot;
		//we need to create a new root node
		rc = AllocateNode(NewRoot);
		if (rc){return rc;}
		
		// we need to take the parent and newNode and distribute the keys across the two
		rc = InsertAndSplitRoot(p, NewNode, NewRoot, k, ptr);
		if(rc){return rc;}
	}
	// if the parent is full and it is an interior node
	else
	{
		SIZE_T newNode;
		// we need to create a new interior node
		rc = AllocateNode(newNode);
		if (rc){return rc;}
		// we need to take the parent and newNode and distribute the keys across the two
		KEY_T newK;
		rc = InsertAndSplitInterior(p, newNode, k, ptr, newK);
		rc = InsertRecur(path, newK, newNode);
		if(rc){return rc;}
	}
	return ERROR_NOERROR;
}

ERROR_T BTreeIndex::InsertAndSplitRoot(SIZE_T &p, SIZE_T &NewInterior, SIZE_T &NewRoot, const KEY_T &k, const SIZE_T &ptr){
	ERROR_T rc;
	KEY_T key;
	//Insert key and val into current root, return pushed up key and pointer to new internal node
	rc = InsertAndSplitInterior(p,NewInterior,k,ptr,key);
	if(rc){return rc;}


	BTreeNode bNewRoot;
	rc = bNewRoot.Unserialize(buffercache, NewRoot);
	if(rc){return rc;}

	bNewRoot.SetKey(0,key);
	bNewRoot.SetPtr(0, p);
	bNewRoot.SetPtr(1, NewInterior);
	bNewRoot.info.numkeys++;

	BTreeNode b;
	rc = b.Unserialize(buffercache, p);
        BTreeNode b2;
        rc = b2.Unserialize(buffercache, NewInterior);

	b.info.nodetype = BTREE_INTERIOR_NODE;
	b2.info.nodetype = BTREE_INTERIOR_NODE;
	bNewRoot.info.nodetype = BTREE_ROOT_NODE;

	rc = b.Serialize(buffercache, p);
	rc = b2.Serialize(buffercache, NewInterior);
	rc = bNewRoot.Serialize(buffercache, NewRoot);
	superblock.info.rootnode = NewRoot;
	
        return rc;
}


ERROR_T BTreeIndex::InsertAndSplitInterior(SIZE_T &I1,
					   SIZE_T &I2,
					   const KEY_T &k,
					   const SIZE_T &ptr,
					   KEY_T &newK)
{
        // Distribute keys and pointers of I1, plus new one, into I1 and I2 (except for middle)

        BTreeNode original;
        BTreeNode newInterior;
        ERROR_T rc;
        
        rc = original.Unserialize(buffercache, I1);
       	if(rc){return rc;}
       	rc = newInterior.Unserialize(buffercache, I2);
       	if(rc){return rc;}
       	
       	SIZE_T firstHalfOfKeys;
	SIZE_T secondHalfOfKeys;
	SIZE_T firstHalfOfptrs;
	SIZE_T secondHalfOfptrs;
	
	// find the index to split on
	firstHalfOfKeys = ceil((original.info.numkeys) / 2);
	secondHalfOfKeys = floor((original.info.numkeys) / 2);
	
	firstHalfOfptrs = ceil((original.info.numkeys + 2) / 2);
	secondHalfOfptrs = floor((original.info.numkeys + 2) / 2);
	
	// set the new leaf's num of keys
	newInterior.info.numkeys = secondHalfOfKeys;
	
	if(rc){return rc;}
	
	SIZE_T offset;
	
	SIZE_T tempPtr;
	KEY_T tempKey; 
       	
       	// split and move half of keys to new node as well as ptrs
       	for(offset=original.info.numkeys-secondHalfOfKeys; offset < original.info.numkeys;offset++)
       	{
       		// get from old interior node
		rc = original.GetKey(offset, tempKey);
		rc = original.GetPtr(offset, tempPtr);
		if(rc){return rc;}
		
		// put into new interior node
		rc = newInterior.SetKey(offset-firstHalfOfKeys,tempKey);
		rc = newInterior.SetPtr(offset-firstHalfOfKeys, tempPtr);
		if(rc){return rc;}
       	}
       	// the last ptr in the original node needs to be moved to the end of the newnode
       	rc = original.GetPtr(original.info.numkeys,tempPtr);
       	rc = newInterior.SetPtr(secondHalfOfKeys,tempPtr);
       	if(rc){return rc;}
       	
       	original.info.numkeys = firstHalfOfKeys;
       	
       	// now find where to put the new key and value
	rc = original.GetKey(firstHalfOfKeys - 1,tempKey);
	if(rc){return rc;}
	
	// write the nodes back to disk
	rc = original.Serialize(buffercache, I1);
	if(rc){return rc;}
	rc = newInterior.Serialize(buffercache,I2);
	if(rc){return rc;}
	
	// insert our key and ptr into the correct 
	if(k < tempKey || k == tempKey)
	{
		rc = FindAndInsertKeyPtr(I1,k,ptr);
	}
	else
	{
		rc = FindAndInsertKeyPtr(I2,k,ptr);
	}
	// send the last key of I1 to InsertRecur
	BTreeNode b;
	rc = b.Unserialize(buffercache, I1);
	rc = b.GetKey(b.info.numkeys-1, newK);
	b.info.numkeys--;
	rc = b.Serialize(buffercache, I1);
	return rc;
        
}


ERROR_T BTreeIndex::FindAndInsertKeyVal(SIZE_T &Node, const KEY_T &key, const VALUE_T &val)
{
	// find the place to insert a new key
	BTreeNode b;
	ERROR_T rc;
	rc = b.Unserialize(buffercache, Node);
	if (rc){return rc;}
	
	SIZE_T offset;
	KEY_T tempKey;
	SIZE_T saveOffset = b.info.numkeys;
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
	for(offset = b.info.numkeys; offset > saveOffset; offset--)
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
	}
	// put the new key in
	b.info.numkeys++;
	swapKV = KeyValuePair(key,val);
	rc = b.SetKeyVal(saveOffset, swapKV);
	
	rc = b.Serialize(buffercache, Node);
	if(rc){ return rc;}
	

	return ERROR_NOERROR;
	
}

ERROR_T BTreeIndex::FindAndInsertKeyPtr(SIZE_T &Node, const KEY_T &key, const SIZE_T &ptr)
{
	// find the place to insert a new key
	BTreeNode b;
	ERROR_T rc;
	rc = b.Unserialize(buffercache, Node);
	if (rc){return rc;}
	
	SIZE_T offset;
	KEY_T tempKey;
	SIZE_T saveOffset = b.info.numkeys;
	SIZE_T tempPtr;
	
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
	for(offset = b.info.numkeys; offset > saveOffset; offset--)
	{
		// get the key and ptr
		rc = b.GetKey(offset-1, tempKey);
		rc = b.GetPtr(offset, tempPtr);
		if (rc){return rc;}
		
		// move them down one from its original spot
		rc = b.SetKey(offset, tempKey);
		rc = b.SetPtr(offset+1, tempPtr);
       		
       		if(rc){return rc;}
	}
	rc = b.SetKey(saveOffset,key);
	rc = b.SetPtr(saveOffset + 1,ptr);
	b.info.numkeys++;
	rc = b.Serialize(buffercache, Node);
	if(rc){return rc;}

	return ERROR_NOERROR;
}

ERROR_T BTreeIndex::InsertInternal(const SIZE_T &Node, const KEY_T &key, const VALUE_T &val)
{

	ERROR_T rc;
	list<SIZE_T> Path;
	// Find the node where the key should be inserted (i.e. leaf node)
	rc = InsertFindNode(Node, key, val, Path);
	
	// Get the node that we to insert into from the Path
	SIZE_T L = Path.back();
	cout << L <<endl;	

	// remove from path the node we insert into
	Path.pop_back();
	BTreeNode b;

	// If L is not full (i.e. the node we insert into)
	if(!isFull(L)){
		cout << "**Node not full" << endl;		
		
		// read data from node
		rc = b.Unserialize(buffercache, L);

		SIZE_T offset;
		SIZE_T saveOffset = b.info.numkeys;
		KEY_T tempKey;
		const KeyValuePair kv = KeyValuePair(key, val);
		KeyValuePair swapKV;
		VALUE_T tempVal;

		cout << "**Num keys in this node: " << b.info.numkeys << "/"<< b.info.GetNumSlotsAsLeaf()<<endl;		
		
		// search for the location to put the key
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

		cout << "**Inserting at position: " << saveOffset << endl;

		// increment the number of keys in the node
		b.info.numkeys++;
		
		// go through the keys and shift accordingly to allocate space for the new key
		for(offset = b.info.numkeys-1; offset > saveOffset; offset--)
		{
			cout << "**Moving key at position "<<offset-1<<" to position "<< offset <<endl;		

			rc = b.GetKey(offset-1, tempKey);
			if(rc){return rc;}

			// if it's the root acting as a leaf, we change its type temporarily so GetVal doesn't freak out
                        if(isRootLeaf(b)){
                                b.info.nodetype = BTREE_LEAF_NODE;
                                
                                rc = b.GetVal(offset-1, tempVal);
                                if(rc){return rc;}
                                
                                swapKV = KeyValuePair(tempKey, tempVal);
                                rc = b.SetKeyVal(offset, swapKV);
                                
                                b.info.nodetype = BTREE_ROOT_NODE;
                        }else{
	                        rc = b.GetVal(offset-1, tempVal);
	                        if(rc){return rc;}
	                        
	                        swapKV = KeyValuePair(tempKey, tempVal);
	                        rc = b.SetKeyVal(offset, swapKV);
			}
			if(rc){return rc;}
		}

		// Now that we've made room, insert our new key/val
		// if it's the root acting as a leaf, we change its type temporarily so SetKeyVal allows it
		if(isRootLeaf(b)){
			b.info.nodetype = BTREE_LEAF_NODE;
			rc = b.SetKeyVal(saveOffset, kv);
			b.info.nodetype = BTREE_ROOT_NODE;
			cout << "**Inserted into rootleaf"<<endl;
		}else{
			rc = b.SetKeyVal(saveOffset, kv);
		}
		if(rc){return rc;}
		
		// write the data back to the disk
		return b.Serialize(buffercache, L);
	}else{
		// the node we want to insert into is full 
		cout << "**The leaf is full" << endl;
		cout << "**Fuckshit"<<endl;	
		cout << "**L in else: "<<L<<endl;

		// read the data from the node
		rc = b.Unserialize(buffercache, L);
		if (rc){return rc;}
		cout << "**Node type is "<<b.info.nodetype << endl;
		switch(b.info.nodetype)
		{
			cout << "**In nodetype switch"<<endl;
			// if its the edge case that the leaf node is really a root
			case BTREE_ROOT_NODE:
			{

				cout << "**In root case"<<endl;
		
				if(isRootLeaf(b)){

					cout << "**Is root leaf"<<endl;
					
					SIZE_T NewRoot;
					SIZE_T NewLeaf;
					
					// make a new leaf and root node
					rc = AllocateNode(NewRoot);
					rc = AllocateNode(NewLeaf);
					
					BTreeNode newRoot = BTreeNode(BTREE_ROOT_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
					BTreeNode newLeaf = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
					
					newRoot.Serialize(buffercache, NewRoot);
					newLeaf.Serialize(buffercache,NewLeaf);
					
					b.info.nodetype = BTREE_LEAF_NODE;
					b.Serialize(buffercache, L);
					
					cout << "**About to go into InsertAndSplitLeaf from InsertInternal" << endl;
					// split our full node with our new leaf node
					// insert our key and value in the appropriate leaf
					rc = InsertAndSplitLeaf(L,NewLeaf,key,val);
					if(rc){return rc;}

					// get the last key in our formerly full node
					KEY_T k;
				        rc = b.GetKey(b.info.numkeys-1,k);
                               	        if(rc){return rc;}
				
					// read data from the new root node
					BTreeNode bNewRoot;
					rc = bNewRoot.Unserialize(buffercache, NewRoot);
					if(rc){return rc;}
					
					// set the key of the root node to be a last key of the formerly
					// full node
					bNewRoot.SetKey(0,k);
					// point to the formerly full leaf node
					bNewRoot.SetPtr(0, L);
					// point to the new leaf node
					bNewRoot.SetPtr(1, NewLeaf);
					
					bNewRoot.info.numkeys++;
                                        
					// write the root back to disk
					rc = bNewRoot.Serialize(buffercache, NewRoot);
					
					// update the superblock to let it know 
					superblock.info.rootnode = NewRoot;
					
				}
				break;
			}
			default:
			{
				// otherwise we must be looking at a leaf node
				SIZE_T L2;
				// allocate space for a new leaf node
				rc = AllocateNode(L2);
				if(rc){return rc;}
				// split the leaf and put half of keys into new leaf node
				rc = InsertAndSplitLeaf(L,L2,key,val);
				// go up the tree to its interior nodes and reshuffle things around
				KEY_T k;
				SIZE_T ptr;
				BTreeNode b2;
				rc = b2.Unserialize(buffercache, L2);
				b2.info.nodetype = BTREE_LEAF_NODE;
				rc = b2.GetKey(0,k);
				rc = b2.GetPtr(0,ptr);
				if(rc){return rc;}
				rc = InsertRecur(Path,k,ptr);
				break;	
			}
		}
	}



	rc = superblock.Serialize(buffercache, superblock_index);

	return rc || ERROR_NOERROR;

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


bool BTreeIndex::isRootLeaf(BTreeNode b){
	return b.info.nodetype == BTREE_ROOT_NODE  && superblock.info.freelist == 2;
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



