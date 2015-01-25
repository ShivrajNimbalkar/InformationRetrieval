package ir.lucene.search.algo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

public class CompareAlgorithms 
{
	public Analyzer analyzer = new StandardAnalyzer();
	public QueryParser queryparser = new QueryParser("TEXT", analyzer);

	private List<SearchAlgorithm> algos = new ArrayList<SearchAlgorithm>();
	
	private String[] runId = null;
	
	public void closeBuffers() throws IOException
	{
		for (SearchAlgorithm searchAlgorithm: algos)
		{
			searchAlgorithm.closeBufferedWriter();
		}
	}
	
	public void loadBufferedWriters(String searchTag) throws IOException
	{
		if ("title".equals(searchTag))
		{
			runId = new String[]{"BM25_short", "VSM_short", "LMDS_short", "LMJMS_short"};
			String[] shortQueryFiles = {"BM25shortQuery.txt", "VSMshortQuery.txt", "LMDSshortQuery.txt", "LMJMSshortQuery.txt"};
			int i = 0;
			for (SearchAlgorithm searchAlgorithm: algos)
			{
				searchAlgorithm.setBufferedWriter(SEARACH_QUERY_OUT_FILE_DIR + shortQueryFiles[i]);
				i++;
			}
		}
		else if ("desc".equals(searchTag))
		{
			runId = new String[]{"BM25_long", "VSM_long", "LMDS_long", "LMJMS_long"};
			String[] longQueryFiles = {"BM25longQuery.txt", "VSMlongQuery.txt", "LMDSlongQuery.txt", "LMJMSlongQuery.txt"};
			int i = 0;
			for (SearchAlgorithm searchAlgorithm: algos)
			{
				searchAlgorithm.setBufferedWriter(SEARACH_QUERY_OUT_FILE_DIR + longQueryFiles[i]);
				i++;
			}
		}
	}
	
	public void initSearchAlgos() throws IOException
	{
		SearchAlgorithm bm25 = new SearchAlgorithm(); // bm25
		bm25.setIndexSearcher(INPUT_INDEX_FILE_NAME, new BM25Similarity());
		algos.add(bm25);
		
		SearchAlgorithm vsm = new SearchAlgorithm(); // vector space model
		vsm.setIndexSearcher(INPUT_INDEX_FILE_NAME, new DefaultSimilarity());
		algos.add(vsm);

		SearchAlgorithm lmds = new SearchAlgorithm();
		lmds.setIndexSearcher(INPUT_INDEX_FILE_NAME, new LMDirichletSimilarity());
		algos.add(lmds);

		SearchAlgorithm lmjms = new SearchAlgorithm();
		lmjms.setIndexSearcher(INPUT_INDEX_FILE_NAME, new LMJelinekMercerSimilarity((float) 0.7));
		algos.add(lmjms);
	}

	/*
	 * method to process query terms and calculate relevance score for input query terms
	*/
	public void getRelevanceScore(String queryString, int trecNumber, String q) throws ParseException, IOException
	{
		Query query = queryparser.parse(QueryParser.escape(queryString));
		
		int i = 0; 
		for (SearchAlgorithm searchAlgorithm: algos)
		{
			searchAlgorithm.searchQuery(query, trecNumber, q, runId[i]);
			i++;
		}
	}

	public static void main(String[] args) throws IOException, ParseException
	{
		ProcessQueryFiles pqf = new ProcessQueryFiles();
		pqf.getQueryScores(INPUT_QUERY_FILE_NAME);
	}	
	
	public static final String SEARACH_QUERY_OUT_FILE_DIR= "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\searchoutput\\";
	public static final String INPUT_QUERY_FILE_NAME = "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\topics.51-100";
	public static final String INPUT_INDEX_FILE_NAME = "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\index\\default";

	private class SearchAlgorithm
	{
		private IndexSearcher searcher = null;
		private BufferedWriter bw = null;
		private IndexReader indexReader = null;

		public void setIndexSearcher(String queryFileName, Similarity similarity) throws IOException
		{
			indexReader =  DirectoryReader.open(FSDirectory.open(new File(queryFileName)));
			searcher = new IndexSearcher(indexReader);
			searcher.setSimilarity(similarity);
		}
		
		public void setBufferedWriter(String fileName) throws IOException
		{
			bw = new BufferedWriter(new FileWriter(new File(fileName)));
			bw.write("QueryId	Q		DocID		Rank		Score			RunID");
			bw.newLine();
		}
		
		public void closeBufferedWriter() throws IOException
		{
			bw.close();
		}
		
		public void recordResults(TopScoreDocCollector collector, int trecNumber, String q, String runId) throws IOException
		{
			ScoreDoc[] docs = collector.topDocs().scoreDocs;
			
			for (int i = 0; i < docs.length; i++) 
			{
				Document doc = searcher.doc(docs[i].doc);
				bw.write(trecNumber+"		"+q+"		"+doc.get("DOCNO") +"		"+ (i+1) +"		"+docs[i].score+"		"+runId);
				bw.newLine();
			}
		}
		
		public void searchQuery(Query query, int trecNumber, String q, String runId) throws IOException
		{
			TopScoreDocCollector collector = TopScoreDocCollector.create(1000, true);
			searcher.search(query, collector);
			recordResults(collector, trecNumber, q, runId);
		}
	}
}

/*
 * class to parse query file and it uses SearchTRECtopics class to calculate relevance score for short and long query. 
 */
class ProcessQueryFiles
{
	private BufferedReader br = null; // read input index
	private CompareAlgorithms compareAlgos = null;
	
	public ProcessQueryFiles()
	{
		compareAlgos = new CompareAlgorithms();
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
		compareAlgos.closeBuffers();
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
			compareAlgos.getRelevanceScore(title, trecNumber, "Q0");
		}
		else if ("desc".equals(searchTag))	// long query
		{
			String desc = getTrecDesc(topic);
			compareAlgos.getRelevanceScore(desc, trecNumber, "Q1");
		}
	}
	
	/*
	 * method to read input/parse input file and to get title and desc from it
	 */
	public void processQueries(String in_filename, String searchTag) throws IOException, ParseException
	{
		br = new BufferedReader(new FileReader(in_filename));
		
		compareAlgos.loadBufferedWriters(searchTag);
		
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
		compareAlgos.initSearchAlgos();
		String[] searchTags = {"title", "desc"};
		
		for (String searchTag: searchTags)
		{
			processQueries(filename, searchTag);
			closeBuffers();
		}
	}	

	public static final String SEARACH_SHORT_QUERY_FILE = "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\searchoutput\\easySearchShortQuery.txt";
	public static final String SEARACH_LONG_QUERY_FILE = "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw2\\searchoutput\\easySearchLongQuery.txt";

}
