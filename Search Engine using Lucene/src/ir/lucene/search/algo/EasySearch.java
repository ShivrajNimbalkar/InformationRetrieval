/*
 * Author: Shivraj Nimbalkar
 */

package ir.lucene.search.algo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class EasySearch 
{
	// map to store term and all the documents which contain it with frequency
	private Map<Term, Map<Integer, Integer>> termFreqMap = new HashMap<Term, Map<Integer, Integer>>();
	// map to store term and its IDF
	private Map<Term, Double> termIDF = new HashMap<Term, Double>();
	
	private IndexReader reader = null;
		
	/*
	 * method to calculate IDF score of all terms in the query 
	 */
	public void calculateTermIDF(Set<Term> queryterms) throws IOException
	{
		int totalDoc = reader.maxDoc(); // total number of documents in the corpus
		
		// calculate idf and term frequency in each document
		for (Term t: queryterms)
		{
			Map<Integer, Integer> termFreq = new HashMap<Integer, Integer>();
			
			// get the term frequency of "new" within each document containing it for <field>TEXT</field>
			DocsEnum de = MultiFields.getTermDocsEnum(reader, MultiFields.getLiveDocs(reader), "TEXT", new BytesRef(t.text()));
			int doc = 0;
			int termDocFreq = 0;
			while ((doc = de.nextDoc()) != DocsEnum.NO_MORE_DOCS)
			{
				termFreq.put(de.docID(), de.freq());
				termDocFreq++;
			}
			
			termFreqMap.put(t, termFreq);
			
			double idf = 0;
			if (termDocFreq > 0)
			{
				idf = Math.log(1+ (totalDoc/termDocFreq));
			}
			termIDF.put(t, idf);
		}
	}
	
	/*
	 * Method to calculate TF-IDF score for each document in segment of the index
	 */
	public void calculateTFIDF(Set<Term> queryterms) throws IOException
	{
		List<DocScore> finalScore = new ArrayList<DocScore>();
		// use DefaultSimilarity.decodeNormValue(..) to normalize document length
		DefaultSimilarity dsimi = new DefaultSimilarity();
		
		// get the segment of the index
		List<AtomicReaderContext> leafContexts = reader.getContext().reader().leaves();

		for (AtomicReaderContext leafContext: leafContexts)
		{
			int startDocNo = leafContext.docBase;
			int numberOfDoc = leafContext.reader().maxDoc();
			int maxSegDoc = startDocNo + numberOfDoc;
			
			for (int docId = startDocNo; docId < maxSegDoc; docId++)
			{
				// get normalized length for each document in the segment
				float normDocLen = dsimi.decodeNormValue(leafContext.reader().getNormValues("TEXT").get(docId - startDocNo));
				//System.out.println("Normalized length for doc ("+docId+") is "+ normDocLen);
				double tf_idf = 0;
				for (Term t: queryterms)
				{
					if (termFreqMap.get(t).containsKey(docId))
					{
						tf_idf += (termFreqMap.get(t).get(docId)/normDocLen) * termIDF.get(t);
					}
				}
				finalScore.add(new DocScore(docId, tf_idf));
			}
		}
		System.out.println("All results are: ");
		
		for(DocScore ds: finalScore)
		{
			System.out.println("DocId = "+ ds.getDocId()+ "  Score = "+ ds.getScore());
		}
	}
	
	/*
	 * driver method to process query terms and calculate relevance score for input query terms
	*/
	public void getRelevanceScore(String queryString) throws ParseException, IOException
	{
		// get the pre-processed query terms
		Analyzer analyzer = new StandardAnalyzer();
		QueryParser queryparser = new QueryParser("TEXT", analyzer);
		Query query = queryparser.parse(queryString);
		Set<Term> queryterms = new LinkedHashSet<Term>();
		query.extractTerms(queryterms);
				
		reader = DirectoryReader.open(FSDirectory.open(new File("E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\index\\default")));
		
		calculateTermIDF(queryterms);
		
		calculateTFIDF(queryterms);
	}
	
	/* main method to get relevance score of all documents for term "new"
	*/
	public static void main(String[] args) throws ParseException, IOException
	{
		EasySearch es = new EasySearch();
		es.getRelevanceScore("new");
	}
	
	/*
	 * Class for storing document id and corresponding score in the final result
	 * */
	private class DocScore
	{
		private int docId;
		private double score;
		
		public DocScore(int docId, double score)
		{
			this.docId = docId;
			this.score = score;
		}
		
		public int getDocId() 
		{
			return docId;
		}
		
		public void setDocId(int docId) 
		{
			this.docId = docId;
		}
		
		public double getScore() 
		{
			return score;
		}
		
		public void setScore(double score) 
		{
			this.score = score;
		}
	}
}
