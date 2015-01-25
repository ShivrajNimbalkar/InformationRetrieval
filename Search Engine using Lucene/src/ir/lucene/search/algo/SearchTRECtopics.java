/*
 * Author: Shivraj Nimbalkar
 */

package ir.lucene.search.algo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class SearchTRECtopics 
{
	// map to store term and all the documents which contain it with frequency
	private Map<Term, Map<Integer, Integer>> termFreqMap = null;
	// map to store term and its IDF
	private Map<Term, Double> termIDF = null;
	private IndexReader reader = null;
	private PriorityQueue<DocScore> pq = null;
	
	private Map<Integer, Float> docLenMap = new HashMap<Integer, Float>();
	
	private int resultSize = 0;
	
	public SearchTRECtopics(int resultSize)
	{
		this.resultSize = resultSize;
	}
	
	public void readIndex() throws IOException
	{
		reader = DirectoryReader.open(FSDirectory.open(new File("E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\index\\default")));
	}
	
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
			while ((null != de) && (doc = de.nextDoc()) != DocsEnum.NO_MORE_DOCS)
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
	 * method to load normalized document length in the hashtable
	 */
	public void loadNormDocLen() throws IOException
	{
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
				docLenMap.put(docId, normDocLen);
			}
		}
	}	
	
	/*
	 *  method to calculate final TF_IDF score for given query terms
	 */
	public void calculateTFIDF(Set<Term> queryterms) throws IOException
	{
		int topResults = 0;
		// find union of all document ids which contain the query terms
		Set<Integer> allTermDocIds = new HashSet<Integer>();

		// merge all doc id's of term, so that only those can be processed together
		for (Term t: queryterms)
		{
			Set<Entry<Integer, Integer>> set= termFreqMap.get(t).entrySet();
			Iterator<Entry<Integer, Integer>> it = set.iterator();
			while(it.hasNext())
			{
				allTermDocIds.add(it.next().getKey());
			}
		}
		
		for (int docId: allTermDocIds)
		{
			double tf_idf = 0;
			
			for (Term t: queryterms)
			{
				if (termFreqMap.get(t).containsKey(docId))
				{
					tf_idf += (termFreqMap.get(t).get(docId)/docLenMap.get(docId)) * termIDF.get(t);
				}
			}
			
			// add resulting document id and its score in the priority queue
			if (topResults != resultSize)
			{
				pq.add(new DocScore(docId, tf_idf));
				topResults++;
			}
			else
			{
				if (pq.peek().getScore() < tf_idf)
				{
					pq.poll();
					pq.add(new DocScore(docId, tf_idf));
				}
			}
		}
	}
	
	/*
	 * method to calculate relevance score for input string
	 */
	public void getRelevanceScore(String queryString) throws ParseException, IOException
	{
		if (resultSize <= 0)
		{
			System.out.println("Please provide the value for top results to print");
			System.exit(-1);
		}
		
		// get the pre-processed query terms
		Analyzer analyzer = new StandardAnalyzer();
		QueryParser queryparser = new QueryParser("TEXT", analyzer);
		Query query = queryparser.parse(QueryParser.escape(queryString));
		Set<Term> queryterms = new LinkedHashSet<Term>();
		query.extractTerms(queryterms);
		
		// priority queue to maintain top results in order
		pq = new PriorityQueue<DocScore>(resultSize, new DocScoreComparator());
		termFreqMap = new HashMap<Term, Map<Integer, Integer>>();
		termIDF = new HashMap<Term, Double>();
		
		calculateTermIDF(queryterms);
		
		calculateTFIDF(queryterms);
		
	}
	
	/*
	 * method to print top results from the priority queue
	 */
	public void printTopResults(int trecNumber, String q, BufferedWriter bw, String runId) throws IOException
	{
		if(!pq.isEmpty())
		{
			DocScore ds = pq.poll();
			int rank = pq.size();
			printTopResults(trecNumber, q, bw, runId);
			bw.write(trecNumber+"	"+	q	+"	"+reader.document(ds.getDocId()).get("DOCNO") +"	"+(rank+1)+"	"+ds.getScore()+"	"+runId);
			bw.newLine();
		}
	}
	
	public static void main(String[] args) throws ParseException, IOException
	{
		ProcessQueryFile processQueries = new ProcessQueryFile(1000);
		processQueries.getQueryScores(INPUT_QUERY_FILE_NAME);
	}
	
	public static final String INPUT_QUERY_FILE_NAME = "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\topics.51-100";
	
	/*
	 * Class for storing document id and corresponding score in the final result
	 * */
	class DocScore
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

	/*
	 * Supporting class for priority queue which maintains data in ascending order
	 * */
	class DocScoreComparator implements Comparator<DocScore>
	{
		@Override
		public int compare(DocScore doc1, DocScore doc2) 
		{
			if (doc1.getScore() > doc2.getScore())
			{
				return 1;
			}
			else if (doc1.getScore() < doc2.getScore())
			{
				return -1;
			}
			else
			{
				return 0;
			}
		}
	}
}

/*
 * class to parse query file and it uses SearchTRECtopics class to calculate relevance score for short and long query. 
 */
class ProcessQueryFile
{
	private SearchTRECtopics search = null;
	private BufferedReader br = null; // read input index
	private BufferedWriter bw = null; // writes output docid and score to file
	
	public ProcessQueryFile(int resultsize) 
	{
		search = new SearchTRECtopics(resultsize);
	}

	/*
	 * Method to extract data between start and end tag of String
	 */
	public String getTagData(String data, String startTag, String endTag)
	{
		Pattern p = Pattern.compile(startTag+"(.+?)"+endTag, Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE );
		Matcher m = p.matcher(data);
		StringBuilder sb = new StringBuilder();
		
		while (m.find())
		{
			sb.append(m.group(2));
		}
		return sb.toString().trim();
	}

	/*
	 * method to get <num> from the topic file
	 */
	public int getTrecNumber(String topic)
	{
		String trecNumber = getTagData(topic, "<num>\\s*(Number:)", "<dom>").trim();
		return Integer.parseInt(trecNumber);
	}
	
	/*
	 * method to get <title> from the topic file
	 */
	public String getTrecTitle(String topic)
	{
		return getTagData(topic, "<title>\\s*(Topic:)", "<desc>").trim();
	}
	
	/*
	 * method to get <desc> from the topic file
	 */
	public String getTrecDesc(String topic)
	{
		return getTagData(topic, "<desc>\\s*(Description:)", "<smry>").trim();
	}
	
	/*
	 * method to close i/o buffers which are used to read and write files
	 */
	public void closeBuffers() throws IOException
	{
		br.close();
		bw.close();
	}
	
	/*
	 * method to get the name of output file for short and long queries
	 */
	public String getOutFile(String searchTag)
	{
		if ("title".equals(searchTag))
		{
			return SEARACH_SHORT_QUERY_FILE;
		}
		else if ("desc".equals(searchTag))
		{
			return SEARACH_LONG_QUERY_FILE;
		}
		return null;
	}
	
	/*
	 * method to get relevance score for short and long queries
	 */
	public void getRelevanceScore(String topic, String searchTag) throws ParseException, IOException
	{
		int trecNumber = getTrecNumber(topic);
		
		if ("title".equals(searchTag))	// short query
		{
			String title = getTrecTitle(topic);
			search.getRelevanceScore(title);
			search.printTopResults(trecNumber, "Q0", bw, "myAlgo_short");
		}
		else if ("desc".equals(searchTag))	// long query
		{
			String desc = getTrecDesc(topic);
			search.getRelevanceScore(desc);
			search.printTopResults(trecNumber, "Q1", bw, "myAlgo_long");
		}
	}
	
	/*
	 * method to read input/parse input file and to get title and desc from it
	 */
	public void processQueries(String in_filename, String searchTag) throws IOException, ParseException
	{
		br = new BufferedReader(new FileReader(in_filename));
		
		String outFileName = null;
		if (null != (outFileName = getOutFile(searchTag)))
		{
			bw = new BufferedWriter(new FileWriter(new File(outFileName)));
			bw.write("QueryId	Q	DocID	Rank	Score	RunID");
			bw.newLine();
		}
		// read index file
		search.readIndex();
		// load normalized document length before processing queries.
		search.loadNormDocLen();
		
		String line = null;
		
		while(null != (line = br.readLine()))
		{
			StringBuilder sb = new StringBuilder();

			// read all content from <top> tags
			while (!line.startsWith("</top>"))
			{
				if (!line.trim().isEmpty())
				{
					sb.append(line).append(" ");
				}
				if (null == (line = br.readLine()))
				{
					break;
				}
			}
			// get score for topic
			if (null != line && line.startsWith("</top>"))
			{
				getRelevanceScore(sb.toString(), searchTag);
			}
		}
	}
	
	/*
	 * method to get scores for different terms in the topics query document
	 */
	public void getQueryScores(String filename) throws IOException, ParseException
	{
		String[] searchTags = {"title", "desc"};
		
		for (String searchTag: searchTags)
		{
			processQueries(filename, searchTag);
			closeBuffers();
		}
	}	

	public static final String SEARACH_SHORT_QUERY_FILE = "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\searchoutput\\myAlgoShortQuery.txt";
	public static final String SEARACH_LONG_QUERY_FILE = "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\searchoutput\\myAlgoLongQuery.txt";

}
