/*
 * Author: Shivraj Nimbalkar
 */
package ir.lucene.generateindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/*
 * Class to generate index on trectext data.
 */
public class GenerateIndex 
{	
	private BufferedReader br = null;

	/*
	 *  Method to index document in corpus
	 */
	public void addDoc(IndexWriter iw, String data) throws IOException
	{
		  Document doc = new Document();

		  doc.add(new StringField("DOCNO", getTagData(data, "DOCNO"), Field.Store.YES));
		  doc.add(new StringField("HEAD", getTagData(data, "HEAD"), Field.Store.YES));
		  doc.add(new StringField("BYLINE", getTagData(data, "BYLINE"), Field.Store.YES));
		  doc.add(new StringField("DATELINE", getTagData(data, "DATELINE"), Field.Store.YES));
		  doc.add(new TextField("TEXT", getTagData(data, "TEXT"), Field.Store.YES));
		  
		  iw.addDocument(doc);
	}
	
	/*
	 * Method to extract data between start and end tag of String
	 */
	public String getTagData(String data, String tag)
	{
		Pattern p = Pattern.compile( "<"+tag+">(.+?)</"+tag+">", Pattern.CASE_INSENSITIVE + Pattern.DOTALL + Pattern.MULTILINE );
		Matcher m = p.matcher(data);
		StringBuilder sb = new StringBuilder();
		
		while (m.find())
		{
			sb.append(data.substring(m.start()+tag.length()+2, m.end()-tag.length()-3).trim()).append(" ");
		}
		return sb.toString().trim();
	}
	
	/*
	 * Method to read <DOC></DOC> from trectext file and add DOC to lucene doc
	 */
	public void indexFile(IndexWriter iw, String filename) throws IOException
	{
		br  = new BufferedReader(new FileReader(filename));
		String line = null;
		
		while(null != (line = br.readLine()))
		{
			StringBuilder sb = new StringBuilder();

			// read all content from <DOC> tags
			while (!line.startsWith("</DOC>"))
			{
				if (!line.startsWith("<DOC>"))
				{
					sb.append(line);
				}
				line = br.readLine();
			}
			// index doc content
			if (line.startsWith("</DOC>"))
			{
				addDoc(iw, sb.toString());
			}
		}
	}
	
	/*
	 * Method to generate index using different analyzers for all trectext files in the specified directory 
	 */
	public void indexData(Analyzer analyzer, String out_index_dir) throws IOException, ParseException
	{
		
		// specify the directory to store the index
		Directory dir = FSDirectory.open(new File(out_index_dir));
		
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_1, analyzer);
		
		iwc.setOpenMode(OpenMode.CREATE);
		
		IndexWriter indexwriter = new IndexWriter(dir, iwc);
		
		File folder = new File("E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw1\\corpus");

		// read data files from directory and process doc from files
		for (File fileEntry: folder.listFiles())
		{
			try
			{
				String filename = folder + "\\" + fileEntry.getName();
				indexFile(indexwriter, filename);	
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
		}
		
		indexwriter.close();
	}
	
	public void indexAnalyzer(String dir_path) throws IOException
	{
		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(dir_path)));
		
		//Print the total number of documents in the corpus
		System.out.println("Total number of documents in the corpus:"+reader.maxDoc());
				
		//Print the number of documents containing the term "new" in <field>TEXT</field>.
		System.out.println("Number of documents containing the term \"new\" for field \"TEXT\": "+reader.docFreq(new Term("TEXT", "new")));
		
		//Print the total number of occurrence of the term "new" across all documents for <field>TEXT</field>.
		System.out.println("Number of occurences of \"new\" in the field \"TEXT\": "+reader.totalTermFreq(new Term("TEXT","new")));
	    
		Terms vocabulary = MultiFields.getTerms(reader, "TEXT");
		
		// count the vocabulary for <field>TEXT</field>
		int count = 0;
		TermsEnum iterator = vocabulary.iterator(null);
		while(iterator.next() != null) 
		{
			count ++;
		}
		
		//Print the size of the vocabulary for <field>TEXT</field>, only available per-segment.
		System.out.println("Size of the vocabulary for this field:"+count);

		//Print the total number of documents that have at least one term for <field>TEXT</field>
		System.out.println("Number of documents that have at least one term for this field: "+vocabulary.getDocCount());
		
		//Print the total number of tokens for <field>TEXT</field>
		System.out.println("Number of tokens for this field: "+vocabulary.getSumTotalTermFreq());
		
		//Print the total number of postings for <field>TEXT</field>
		System.out.println("Number of postings for this field: "+vocabulary.getSumDocFreq());
		
		//Print the vocabulary for <field>TEXT</field>
/*		TermsEnum iterator = vocabulary.iterator(null);
		BytesRef byteRef = null;
		
		System.out.println("\n*******Vocabulary-Start**********");
		
		while((byteRef = iterator.next()) != null) 
		{
			String term = byteRef.utf8ToString();
			System.out.print(term+"\t");
		}
		
		System.out.println("\n*******Vocabulary-End**********");
*/		reader.close();
	}

	public static void main(String[] args) throws IOException, ParseException
	{
		GenerateIndex indexer = new GenerateIndex();
		
		//	Specify the analyzer for tokenizing text.
	    //	The same analyzer should be used for indexing and searching
		StandardAnalyzer analyzer = new StandardAnalyzer();
		indexer.indexData(analyzer, STD_ANALYZER_INDEX_DIR);
		indexer.indexAnalyzer(STD_ANALYZER_INDEX_DIR);
	}
	
	public static final String STD_ANALYZER_INDEX_DIR = "E:\\IUB_all\\Fall-2014\\Info Retrieval\\hw1\\std_index";
}
