/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package btree;

import java.io.*;
import java.util.Currency;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;

/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	/**
	 * It causes a structured trace to be written to a file. This output is used to
	 * drive a visualization tool that shows the inner workings of the b-tree during
	 * its operations.
	 *
	 * @param filename input parameter. The trace file name
	 * @exception IOException error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	/**
	 * Stop tracing. And close trace file.
	 *
	 * @exception IOException error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this btree
	 *         file.
	 */
	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename) throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist; this
	 * opens it.
	 *
	 * @param filename the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException  can not ger the file from DB
	 * @exception PinPageException       failed when pin a page
	 * @exception ConstructPageException BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException, PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BTreeFile's header page; - headerPage,
		 * headerPageId valid and pinned - dbname contains a copy of the name of the
		 * database
		 */
	}

	/**
	 * if index file exists, open it; else create it.
	 *
	 * @param filename       file name. Input parameter.
	 * @param keytype        the type of key. Input parameter.
	 * @param keysize        the maximum size of a key. Input parameter.
	 * @param delete_fashion full delete or naive delete. Input parameter. It is
	 *                       either DeleteFashion.NAIVE_DELETE or
	 *                       DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException  can not get file
	 * @exception ConstructPageException page constructor failed
	 * @exception IOException            error from lower layer
	 * @exception AddFileEntryException  can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize, int delete_fashion)
			throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 *
	 * @exception PageUnpinnedException       error from the lower layer
	 * @exception InvalidFrameNumberException error from the lower layer
	 * @exception HashEntryNotFoundException  error from the lower layer
	 * @exception ReplacerException           error from the lower layer
	 */
	public void close()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 *
	 * @exception IOException              error from the lower layer
	 * @exception IteratorException        iterator error
	 * @exception UnpinPageException       error when unpin a page
	 * @exception FreePageException        error when free a page
	 * @exception DeleteFileEntryException failed when delete a file from DM
	 * @exception ConstructPageException   error in BT page constructor
	 * @exception PinPageException         failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException, UnpinPageException, FreePageException,
			DeleteFileEntryException, ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException, IteratorException, PinPageException,
			ConstructPageException, UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page, headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException, PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */);

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/**
	 * insert record with the given key and rid
	 *
	 * @param key the key of the record. Input parameter.
	 * @param rid the rid of the record. Input parameter.
	 * @exception KeyTooLongException     key size exceeds the max keysize.
	 * @exception KeyNotMatchException    key is not integer key nor string key
	 * @exception IOException             error from the lower layer
	 * @exception LeafInsertRecException  insert error in leaf page
	 * @exception IndexInsertRecException insert error in index page
	 * @exception ConstructPageException  error in BT page constructor
	 * @exception UnpinPageException      error when unpin a page
	 * @exception PinPageException        error when pin a page
	 * @exception NodeNotMatchException   node not match index page nor leaf page
	 * @exception ConvertException        error when convert between revord and byte
	 *                                    array
	 * @exception DeleteRecException      error when delete in index page
	 * @exception IndexSearchException    error when search
	 * @exception IteratorException       iterator error
	 * @exception LeafDeleteException     error when delete in leaf page
	 * @exception InsertException         error when insert in index page
	 */
	public void insert(KeyClass key, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException {

		if (trace != null) {
			trace.writeBytes("INSERT " + rid.pageNo + " " + rid.slotNo + " " + key + lineSep);
			trace.writeBytes("DO" + lineSep);
			trace.flush();
		}
		// checking header page if tree exists
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			// No tree. Creating leaf page
			BTLeafPage newRootLeafPage = new BTLeafPage(headerPage.get_keyType());
			PageId newRootLeafPageId = newRootLeafPage.getCurPage();

			// setting the next and previous to INVALID_PAGE
			newRootLeafPage.setNextPage(new PageId(INVALID_PAGE));
			newRootLeafPage.setPrevPage(new PageId(INVALID_PAGE));

			// inserting the new record on newRootLeafPage
			newRootLeafPage.insertRecord(key, rid);

			// unpinning page since dirty
			unpinPage(newRootLeafPageId, true);

			// updating headerPage
			updateHeader(newRootLeafPageId);

			// insert complete
			return;
		}

		// root not empty. Tree exists
		// inserting into rootPage
		KeyDataEntry newRootEntry = _insert(key, rid, headerPage.get_rootId());

		// checking if split occured
		// newRootEntry = null then no split occured
		// newRootEntry !=null then split occured. Entry needs to be added in root.

		if (newRootEntry != null) {
			// creating new page entry
			BTIndexPage newRootIndexPage = new BTIndexPage(headerPage.get_keyType());
			PageId newRootIndexPageId = newRootIndexPage.getCurPage();

			// inserting the returned key to that
			newRootIndexPage.insertKey(newRootEntry.key, ((IndexData)newRootEntry.data).getData());

			// after split the root split
			// set newRootIndexPage's previous to old root page
			newRootIndexPage.setPrevPage(headerPage.get_rootId());

			// unpinning the newRootIndexPage because it is dirty
			unpinPage(newRootIndexPageId);

			// updating header with new page
			updateHeader(newRootIndexPageId);
		}
	}

	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, LeafDeleteException, ConstructPageException, DeleteRecException,
			IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException, IteratorException,
			IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException {

		Page page;
		KeyDataEntry upEntry;
		BTSortedPage currentPage;

		page = pinPage(currentPageId);
		currentPage = new BTSortedPage(page, headerPage.get_keyType());

		// checking currentPage type
		if (currentPage.getType() == NodeType.INDEX) {
			BTIndexPage currentIndexPage = new BTIndexPage(page, headerPage.get_keyType());
			PageId currentIndexPageId = currentPageId;

			PageId nextPageId = currentIndexPage.getPageNoByKey(key);

			// unpinning page to recurse the tree
			unpinPage(currentIndexPageId);
			// recursing the tree to insert
			// returns key to be inserted in currentPage if child split.
			upEntry = _insert(key, rid, nextPageId);

			// if upEntry == null: no split has occured.
			// insert complete.
			// if upEntry != null: split occured in the child subtree
			// key in upEntry has to be inserted in currentPage.

			if (upEntry == null) {
				// no split occured. Insert complete. Returning null.
				return null;
			}

			currentIndexPage = new BTIndexPage(pinPage(currentIndexPageId), headerPage.get_keyType());

			// upEntry!=null
			// key in upEntry needs to be inserted in currentIndexPage

			// check whether currentIndexPage has space
			if (currentIndexPage.available_space() >= BT.getKeyDataLength(upEntry.key, NodeType.INDEX)) {
				// space available in currentIndexPage. Inserting here.
				currentIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());

				// unpinning the page after write
				unpinPage(currentIndexPageId, true);

				// insert complete. return null
				return null;
			}

			// currentIndexPage does not have enough space
			// creating newIndexPage.
			BTIndexPage newIndexPage = new BTIndexPage(headerPage.get_keyType());
			PageId newIndexPageId = newIndexPage.getCurPage();

			KeyDataEntry tmpkeyEntry;
			RID delRID = new RID();

			// looping through the keys in currentIndexPage
			for (tmpkeyEntry = currentIndexPage.getFirst(delRID); tmpkeyEntry != null; tmpkeyEntry = currentIndexPage
					.getNext(delRID)) {

				// inserting tmpkeyEntry to newIndexPage
				newIndexPage.insertKey(tmpkeyEntry.key, ((IndexData) tmpkeyEntry.data).getData());
				// deleting key from currentIndexPage
				currentIndexPage.deleteSortedRecord(delRID);
			}

			// currentIndexPage empty
			// inserting records to currentIndexPage to make the number of keys equal
			RID firstRid = new RID();
			KeyDataEntry undoEntry = null;
			for (tmpkeyEntry = currentIndexPage.getFirst(firstRid); (currentIndexPage.available_space() > newIndexPage
					.available_space()); tmpkeyEntry = newIndexPage.getFirst(firstRid)) {

				undoEntry = tmpkeyEntry;

				currentIndexPage.insertKey(tmpkeyEntry.key, ((IndexData) tmpkeyEntry.data).getData());

				newIndexPage.deleteSortedRecord(firstRid);
			}

			// undoing the last entry
			if (currentIndexPage.available_space() < newIndexPage.available_space()) {
				newIndexPage.insertKey(undoEntry.key, ((IndexData) undoEntry.data).getData());

				// removing from currentIndexPage
				currentIndexPage
						.deleteSortedRecord(new RID(currentIndexPage.getCurPage(), currentIndexPage.getSlotCnt() - 1));
			}

			// deciding where the upEntry key should get inserted
			// in newIndexPage or currentIndexPage

			if (BT.keyCompare(upEntry.key, tmpkeyEntry.key) >= 0) {
				// the upEntry key goes in the newIndexPage
				newIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());
			} else {
				// the new entry goes in currentIndexPage
				currentIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());

				int i = currentIndexPage.getSlotCnt() - 1;

				tmpkeyEntry = BT.getEntryFromBytes(currentIndexPage.getpage(), currentIndexPage.getSlotOffset(i),
						currentIndexPage.getSlotLength(i), headerPage.get_keyType(), NodeType.INDEX);

				newIndexPage.insertKey(tmpkeyEntry.key, ((IndexData) tmpkeyEntry.data).getData());

				currentIndexPage.deleteSortedRecord(new RID(currentIndexPage.getCurPage(), i));
			}

			// unpinning currentIndexPage as it is dirty
			unpinPage(currentIndexPageId, true);

			// as delRID got removed it needs to be included in the parent indexPage
			// using upEntry
			upEntry = newIndexPage.getFirst(delRID);

			// setting the prevPageId of newIndexPage to the pageId
			// of deleted entry
			newIndexPage.setPrevPage(((IndexData) upEntry.data).getData());

			// delete first record on newIndexPage
			newIndexPage.deleteSortedRecord(delRID);

			((IndexData) upEntry.data).setData(newIndexPageId);

			return upEntry;

		} else if (currentPage.getType() == NodeType.LEAF) {
			// node to be inserted is a leaf node
			BTLeafPage currentLeafPage = new BTLeafPage(page, headerPage.get_keyType());
			PageId currentLeafPageId = currentPageId;

			// checking if space is available in the currentLeafPage
			if (currentLeafPage.available_space() >= BT.getKeyDataLength(key, NodeType.LEAF)) {
				// no splitting necessary
				// insert key in the same leaf page
				currentLeafPage.insertRecord(key, rid);

				// unpinning currentLeafPage as dirty
				unpinPage(currentLeafPageId, true);

				return null;
			}

			// no space in currentLeafPage.

			// currentLeafPage will need to be split

			// creating newLeafPage to split the records of currentLeafPage

			BTLeafPage newLeafPage = new BTLeafPage(headerPage.get_keyType());

			PageId newLeafPageId = newLeafPage.getCurPage();

			// setting newLeafPage's next as currentLeafPage's nextpage
			newLeafPage.setNextPage(currentLeafPage.getNextPage());
			// setting newLeafPage's prevPage as currentLeafPage
			newLeafPage.setPrevPage(currentLeafPageId);
			// currentLeafPage's next set to newLeafPageId
			currentLeafPage.setNextPage(newLeafPageId);

			// changing the prev pointer on following leaf page
			PageId rightPageId = newLeafPage.getNextPage();
			if (rightPageId.pid != INVALID_PAGE) {
				// currentLeafPage was not right-most leaf
				BTLeafPage rightLeafPage = new BTLeafPage(rightPageId, headerPage.get_keyType());
				// chaning the right leaf's prev to point to newLeafPage
				rightLeafPage.setPrevPage(newLeafPageId);

				// unpining as it is dirty
				unpinPage(rightPageId, true);
			}

			// transferring records from currentLeafPage to newLeafPage
			KeyDataEntry tmpkeyEntry;
			RID delRid = new RID();

			for (tmpkeyEntry = currentLeafPage.getFirst(delRid); tmpkeyEntry != null; tmpkeyEntry = currentLeafPage
					.getFirst(delRid)) {
				// inserting into newLeafPage
				newLeafPage.insertRecord(tmpkeyEntry.key, ((LeafData) tmpkeyEntry.data).getData());
				// deleting from currentLeafPage
				currentLeafPage.deleteSortedRecord(delRid);
			}

			// transferring back some records from newLeafPage to currentLeafPage
			KeyDataEntry undoEntry = null;
			for (tmpkeyEntry = newLeafPage.getFirst(delRid); newLeafPage.available_space() < currentLeafPage
					.available_space(); tmpkeyEntry = newLeafPage.getFirst(delRid)) {
				undoEntry = tmpkeyEntry;
				currentLeafPage.insertRecord(tmpkeyEntry.key, ((LeafData) tmpkeyEntry.data).getData());

				newLeafPage.deleteSortedRecord(delRid);
			}

			if (undoEntry != null && BT.keyCompare(key, undoEntry.key) > 0) {
				// last entry goes in newLeafRecord
				// undo the final record
				if (currentLeafPage.available_space() < newLeafPage.available_space()) {
					newLeafPage.insertRecord(undoEntry.key, ((LeafData) undoEntry.data).getData());

					currentLeafPage.deleteSortedRecord(new RID(currentLeafPage.getCurPage(), currentLeafPage.getSlotCnt() - 1));
				}
			}

			if (undoEntry != null && BT.keyCompare(key, undoEntry.key) >= 0) {
				// key goes into newLeafPage
				newLeafPage.insertRecord(key, rid);
			} else {
				// key goes into currentLeafPage
				currentLeafPage.insertRecord(key, rid);
			}

			// unpinning currentLeafPage since dirty
			unpinPage(currentLeafPageId, true);

			// copying upEntry to send up the level
			tmpkeyEntry = newLeafPage.getFirst(delRid);
			upEntry = new KeyDataEntry(tmpkeyEntry.key, newLeafPageId);

			// unpinning newLeafPage since dirty
			unpinPage(newLeafPageId);

			return upEntry;
		} else {
			throw new InsertException(null, "");
		}
	}

	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry; it
	 * is not the id of the data entry)
	 *
	 * @param key the key in pair <key, rid>. Input Parameter.
	 * @param rid the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException    neither full delete nor naive delete
	 * @exception LeafRedistributeException redistribution error in leaf pages
	 * @exception RedistributeException     redistribution error in index pages
	 * @exception InsertRecException        error when insert in index page
	 * @exception KeyNotMatchException      key is neither integer key nor string
	 *                                      key
	 * @exception UnpinPageException        error when unpin a page
	 * @exception IndexInsertRecException   error when insert in index page
	 * @exception FreePageException         error in BT page constructor
	 * @exception RecordNotFoundException   error delete a record in a BT page
	 * @exception PinPageException          error when pin a page
	 * @exception IndexFullDeleteException  fill delete error
	 * @exception LeafDeleteException       delete error in leaf page
	 * @exception IteratorException         iterator error
	 * @exception ConstructPageException    error in BT page constructor
	 * @exception DeleteRecException        error when delete in index page
	 * @exception IndexSearchException      error in search in index pages
	 * @exception IOException               error from the lower layer
	 *
	 */
	public boolean Delete(KeyClass key, RID rid)
			throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
			KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException, IOException {
		if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
			return NaiveDelete(key, rid);
		else
			throw new DeleteFashionException(null, "");
	}

	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 * 
	 * find left-most occurrence of `lo_key', going all the way left if lo_key is
	 * null.
	 * 
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 * 
	 * Since we allow duplicates, this must "go left" as described in the text (for
	 * the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way left
	 * if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
	 */

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException, IteratorException, KeyNotMatchException,
			ConstructPageException, PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null && BT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (BT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}

	/*
	 * Status BTreeFile::NaiveDelete (const void *key, const RID rid)
	 * 
	 * Remove specified data entry (<key, rid>) from an index.
	 * 
	 * We don't do merging or redistribution, but do allow duplicates.
	 * 
	 * Page containing first occurrence of key `key' is found for us by
	 * findRunStart. We then iterate for (just a few) pages, if necesary, to find
	 * the one containing <key,rid>, which we then delete via
	 * BTLeafPage::delUserRid.
	 */

	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException, ConstructPageException, IOException,
			UnpinPageException, PinPageException, IndexSearchException, IteratorException {

		BTLeafPage leafPage;
		RID currentRid = new RID();
		PageId nextpage;
		KeyDataEntry entry;

		leafPage = findRunStart(key, currentRid);

		// check if start page is null
		if (leafPage == null)
			return false;

		entry = leafPage.getCurrent(currentRid);

		// traversing the tree
		while (true) {
			while (entry == null) {
				nextpage = leafPage.getNextPage();
				unpinPage(leafPage.getCurPage());

				// end of leaf list reached
				if (nextpage.pid == INVALID_PAGE) {
					return false;
				}

				leafPage = new BTLeafPage(pinPage(nextpage), headerPage.get_keyType());
				entry = leafPage.getFirst(new RID());
			}

			// checking if leaf has to be traversed anymore
			if (BT.keyCompare(key, entry.key) > 0) {
				// current key is greater than required key
				break;
			}

			KeyDataEntry keyToDelete = new KeyDataEntry(key, rid);

			if (leafPage.delEntry(keyToDelete) == true) {
				// found the <key,rid> and deleted it.
				// end search here
				// unpinning the page to since it is dirty
				unpinPage(leafPage.getCurPage(), true);

				// successfully deleted.
				return true;
			}

			// end of current page reached. Shifting to next page
			nextpage = leafPage.getNextPage();
			unpinPage(leafPage.getCurPage(), true);

			// assigning nextPage in leafPage
			leafPage = new BTLeafPage(pinPage(nextpage), headerPage.get_keyType());

			entry = leafPage.getFirst(currentRid);
		}
		// key not found. Returning unsuccessfully
		unpinPage(leafPage.getCurPage());
		return false;
	}

	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null scan
	 * the whole index (2) lo_key = null, hi_key!= null range scan from min to the
	 * hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key to max (4)
	 * lo_key!= null, hi_key!= null, lo_key = hi_key exact match ( might not unique)
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key range scan from lo_key to
	 * hi_key
	 *
	 * @param lo_key the key where we begin scanning. Input parameter.
	 * @param hi_key the key where we stop scanning. Input parameter.
	 * @exception IOException            error from the lower layer
	 * @exception KeyNotMatchException   key is not integer key nor string key
	 * @exception IteratorException      iterator error
	 * @exception ConstructPageException error in BT page constructor
	 * @exception PinPageException       error when pin a page
	 * @exception UnpinPageException     error when unpin a page
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key) throws IOException, KeyNotMatchException,
			IteratorException, ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id)
			throws IOException, IteratorException, ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

}
