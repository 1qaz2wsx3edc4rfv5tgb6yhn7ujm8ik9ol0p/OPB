package opb_patchjob;

import java.io.*; 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.MalformedURLException;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * To read from shared drive, process patching file per offer inst id and output when done
 *<p>
 * Environment Variables:<br>
 * ======================<br>
 * 1. OPB_CFG      : full path name of config folder<br>
 * 2. ABPOPS_SHL   : full path name of shell (abp_dbconnection.ksh) to get DB username/password<br>
 * 3. COMM_FTP     : full path name of shell (FTP_T_M1IS_BILLSYS_BILLING.DAT) to get FTP username/password<br> 
 *<p>
 * Change History:<br>
 * ===============<br>
 * 02 Jun 2015   GHKOH  First version
 * <p>
 * @author Ben Koh
 * @version 1.0
 * @param nil
*/

public class opb_patchjob {

	/* Global parameters */
	private static final Logger logger = LogManager.getLogger("logfile");
	private static String enable_commit = "";
	private Dblink cust1 = new Dblink();
	private Dblink main1 = new Dblink();
	private String sharedDir;
	private NtlmPasswordAuthentication smbAuth = null;
	private Properties p = new Properties();
	
	private PrintWriter writer = null;
	private BufferedReader reader = null; 
	
	/***
	 * Class to handle database instance
	 */
	public class Dblink {
		// Database 
		// These variables will be set via config file.
		private Connection dbconn;
		private String jdbcURL;
		private String dbuser, dbpasswd;
		private Statement statement;
		
		public Dblink() {}
		
		protected void setJDBCURL(String jdbcURL) { this.jdbcURL = jdbcURL; }
		protected String getJDBCURL() { return this.jdbcURL==null ? "" : this.jdbcURL; }
		
		protected void setDBUser(String dbuser) { this.dbuser = dbuser; }
		protected String getDBUser() { return this.dbuser==null ? "" : this.dbuser; }
		
		protected void setDBPassword(String dbpasswd) { this.dbpasswd = dbpasswd; }
		protected String getDBPassword() { return this.dbpasswd==null ? "" : this.dbpasswd; }
		
		protected void setStatement(Statement statement) { this.statement = statement; }
		protected Statement getStatement() { return this.statement; }
		
		protected void setConnection(Connection dbconn) { this.dbconn = dbconn; }
		protected Connection getConnection() { return this.dbconn; }

		protected void addBatch(String sql) throws SQLException { 
			this.statement.addBatch(sql); 
		}
		
		protected void clearBatch() throws SQLException { this.statement.clearBatch(); }
		
		protected int[] executeBatch() throws SQLException {
			return this.statement.executeBatch(); 
		}
		
		protected ResultSet executeQuery(String sql) throws SQLException {
			this.statement.clearBatch();
			return this.statement.executeQuery(sql);
		}
		
		protected void login() throws SQLException {
		  	// Register the Oracle JDBC driver
		  	logger.debug("Loading JDBC Driver -> oracle.jdbc.OracleDriver");
		  	DriverManager.registerDriver (new oracle.jdbc.OracleDriver());

		  	// Connect to the PS DB
		  	logger.debug("Connecting to -> " + jdbcURL);
		  	dbconn = DriverManager.getConnection(jdbcURL, dbuser, dbpasswd);
		  	logger.debug("Connected as -> " + dbuser);

		  	statement = dbconn.createStatement();
		  	dbconn.setAutoCommit(false);
	    }

	  	protected void logout() {
	  		try {
	  			if (statement != null) statement.close();
	  			// anything not explicitly committed will be rollback
	  			if (dbconn != null) {dbconn.rollback(); dbconn.close();}
	  		} catch (Exception e) {
	  			logger.fatal("Fatal Error logging out (" + dbuser + "@" + jdbcURL + ") : ",e);
	  		}
	  		logger.debug("Logout " + dbuser + "@" + jdbcURL +" Success");
	  	}

	  	protected void commit() throws SQLException {
	  		if (dbconn != null) dbconn.commit();
	  		logger.debug("Transaction committed");
	  	}
	}

	/***
	 * Class to store offer details
	 * Used Strings for Date as quite troublesome with storing Date Timestamp
	 */
	public class Offer {
		private String recordType, offerInstId, action, remarks, offerType, status, nti;
		private String activeDt, inactiveDt, accStartDt, svcStartDt, svcEndDt, nActiveDt, nInactiveDt;

		protected void setRecordType(String recordType) { this.recordType = recordType; }
		protected String getRecordType() { return (this.recordType==null || this.recordType.isEmpty()) ? "null" : this.recordType; }
		
		protected void setOfferInstId(String offerInstId) { this.offerInstId = offerInstId; }		
		protected String getOfferInstId() { return (this.offerInstId==null || this.offerInstId.isEmpty()) ? "null" : this.offerInstId; }
		
		protected void setAction(String action) { this.action = action; }		
		protected String getAction() { return (this.action==null || this.action.isEmpty()) ? "null" : this.action; }
		
		protected void setActiveDt(String activeDt) { this.activeDt = activeDt; }
		protected String getActiveDt() { return (this.activeDt==null || this.activeDt.isEmpty()) ? "null" : this.activeDt; }
		
		protected void setInactiveDt(String inactiveDt) { this.inactiveDt = inactiveDt; }		
		protected String getInactiveDt() { return (this.inactiveDt==null || this.inactiveDt.isEmpty()) ? "null" : this.inactiveDt; }
		
		protected void setNActiveDt(String nActiveDt) { this.nActiveDt = nActiveDt; }
		protected String getNActiveDt() { return (this.nActiveDt==null || this.nActiveDt.isEmpty()) ? "null" : this.nActiveDt; }
		
		protected void setNInactiveDt(String nInactiveDt) { this.nInactiveDt = nInactiveDt; }
		protected String getNInactiveDt() { return (this.nInactiveDt==null || this.nInactiveDt.isEmpty()) ? "null" : this.nInactiveDt; }
		
		protected void setRemarks(String remarks) { this.remarks = remarks; }
		protected String getRemarks() { return (this.remarks==null || this.remarks.isEmpty()) ? "null" : this.remarks; }
		
		protected void setOfferType(String offerType) { this.offerType = offerType; }
		protected String getOfferType() { return (this.offerType==null || this.offerType.isEmpty()) ? "null" : this.offerType; }
		
		protected void setAccStartDt(String accStartDt) { this.accStartDt = accStartDt; }
		protected String getAccStartDt() { return (this.accStartDt==null || this.accStartDt.isEmpty()) ? "null" : this.accStartDt; }
		
		protected void setSvcStartDt(String svcStartDt) { this.svcStartDt = svcStartDt; }
		protected String getSvcStartDt() { return (this.svcStartDt==null || this.svcStartDt.isEmpty()) ? "null" : this.svcStartDt; }
		
		protected void setSvcEndDt(String svcEndDt) { this.svcEndDt = svcEndDt; }
		protected String getSvcEndDt() { return (this.svcEndDt==null || this.svcEndDt.isEmpty()) ? "null" : this.svcEndDt; }
		
		protected void setStatus(String status) { this.status = status; }
		protected String getStatus() { return (this.status==null || this.status.isEmpty()) ? "null" : this.status; }

		protected void setNTI(String nti) { this.nti = nti; }
		protected String getNTI() { return (this.nti==null || this.nti.isEmpty()) ? "null" : this.nti; }
	}

/**
   * Entry.
   * @param args 
   * @return Nothing.
*/
	public static void main(String[] args) {
		/* set global parameters */
		enable_commit = "N";
		int return_code = 0;
		opb_patchjob opb = new opb_patchjob();
		
		try {
			//logger.info("Run date: "+billrundate+", mode: "+runmode);
			logger.info("***\tJOB START TIME: "+ (new Date())+"\t***");

			opb.init();
			opb.login();
			opb.setFTPAuth(opb.p.getProperty("ntuser").trim(), opb.p.getProperty("ntpass").trim());
    		
			logger.info("Patching with user " + opb.cust1.getDBUser());
			logger.debug("Accessing shared directory at : " + opb.sharedDir);
			
			if (opb.getSmbFiles(opb.sharedDir).size()==0)
				logger.info("No files today: " + (new Date()));
			else {
				logger.info("Fetched " + opb.getSmbFiles(opb.sharedDir).size() + " files to process");
				for (SmbFile f : opb.getSmbFiles(opb.sharedDir)) {
					logger.info("Processing file " + f.getCanonicalPath() + " at " + (new Date()));
					opb.readFile(f);

					if (enable_commit.equals("Y"))
						opb.commit();   // commit only when explicitly set to Y
					
					logger.debug("Processed file " + f.getCanonicalPath() + " at " + (new Date()));
				}
			}

			logger.info("***\tJOB COMPLETED "+ (new Date()) + "\t***");
		}
		catch (MalformedURLException e) {
			logger.fatal("Fatal URL Error while getting FTP credentials:\t"+e.getMessage());
			logger.debug(e);
			return_code = 1;
		}
		catch (SmbException e) {
			logger.fatal("Fatal SMB Error while getting FTP credentials:\t" + e.getMessage());
			logger.debug(e);
			return_code = 1;			
		}
		catch (IOException e) {
	        logger.fatal("Fatal IO Error:\t" + e.getMessage());
	        logger.debug(e);
	        return_code = 1;
		}
		catch (InterruptedException e) {
			logger.fatal("Fatal Interrupted Error while getting database credentials:\t" + e.getMessage());
			logger.debug(e);
			return_code = 1;
		}
		catch (Exception e) {
	        logger.fatal("Fatal Error:", e.toString());
	        logger.debug(e);
	        return_code = 1;
	    }
		finally {
			try {
				opb.logout();
				opb.closeFiles();
			} catch (Exception e) {
				logger.error("Error: interface(s) not closed ", e.getMessage());
				logger.debug(e);
			}
			System.exit(return_code);
		}
	}
	
	/*** Constructor */
	public opb_patchjob() {}

	public void closeFiles() throws IOException {
		if (reader!=null) reader.close();
		if (writer!=null) writer.close();
	}
	
	/*** Initialise from properties file. */
    public void init() throws IOException, InterruptedException {
    	String configdir = System.getenv("OPB_CFG");
    	if (configdir==null)  {
    		p.load(new FileInputStream("C:\\Users\\t_kohgh\\workspace\\opb_patchjob.cfg"));
    		logger.debug("Loaded hardcoded property file opb_patchjob.cfg");
    	}
    	else {
    		p.load(new FileInputStream(configdir+File.separator+"opb_patchjob.cfg"));
    		logger.debug("Loaded property file @" + configdir+File.separator+"opb_patchjob.cfg");
    	}

    	String override = p.getProperty("db_override").toUpperCase();
		String custkey = p.getProperty("cust");
		cust1.setJDBCURL(p.getProperty(custkey));
		String mainkey = p.getProperty("main");
		main1.setJDBCURL(p.getProperty(mainkey));
		
    	if (override.equals("Y")) {
    		cust1.setDBUser(p.getProperty("cust_db_user"));
    		cust1.setDBPassword(p.getProperty("cust_db_password"));
    		main1.setDBUser(p.getProperty("main_db_user"));
    		main1.setDBPassword(p.getProperty("main_db_password"));
    	}
    	else {
    		String custlogin[] = getEncryptedUserPass(custkey).split("/");
    		if (custlogin.length<2) throw new IOException("C1CUSTDB credentials not defined");
    		else if (cust1.getDBUser().isEmpty()) throw new IOException("C1CUSTDB Login not defined");
    		else {
    			cust1.setDBUser(custlogin[0]);
    			cust1.setDBPassword(custlogin[1]);
    		}
    		 
    		String mainlogin[] = getEncryptedUserPass(mainkey).split("/");
    		if (mainlogin.length<2) throw new IOException("C1MAINDB credentials not defined");
    		else if (main1.getDBUser().isEmpty()) throw new IOException("C1MAINDB Login not defined");
    		else {
    			main1.setDBUser(mainlogin[0]);
    			main1.setDBPassword(mainlogin[1]);
    		}
    	}

        // load Shared drive path
        if (p.getProperty("SharedDirectory").equals("")) throw new IOException("Shared directory location not defined");
        else if (p.getProperty("SharedDirectory")!=null) sharedDir = p.getProperty("SharedDirectory");

        logger.debug("SharedDirectory set to: " + sharedDir);

        enable_commit = p.getProperty("enable_commit").toUpperCase();
        logger.debug("Commit is set to: " + enable_commit);

        logger.debug("Done directory is set to: " + p.getProperty("doneDir"));
    }

    public void setFTPAuth(String user, String pass) {
    	smbAuth = new NtlmPasswordAuthentication("",user,pass);

    	// not reading from file anymore, since the unable to read from file
    	/*
    	BufferedReader br = null;
    	try {
    		String path = System.getenv("COMM_FTP");
    		br = new BufferedReader(new FileReader(path+"/FTP_T_M1IS_BILLSYS_BILLING.DAT"));
    		logger.info("Found file " + path + "/FTP_T_M1IS_BILLSYS_BILLING.DAT");
    		String line = null;
    		while ((line=br.readLine())!=null) {
    			if (line.trim().startsWith("user")) {
    				loginpass = line;
    				break;
    			}
    		}
    	}
    	catch (FileNotFoundException e) { logger.warn("FTP script not found: "+e.getMessage()); }
    	catch (IOException e) { logger.warn("FTP script cannot be read: "+e.getMessage()); }
    	finally {
    		try {
    			if (br!=null) br.close();
    		} catch (IOException e) { 
    			logger.warn("ERROR closing FTP script: ", e.getMessage());
    		}
    	}
    	
		if (loginpass.length()==0) {
    		//smbAuth = new NtlmPasswordAuthentication("","t_kohgh","Pokemon01");
			logger.error("No FTP credentials found");
    	} else {
    		logger.trace("FTP user obtained: " + (loginpass.split(" "))[1].trim());
    		smbAuth = new NtlmPasswordAuthentication("",(loginpass.split(" "))[1].trim(),(loginpass.split(" "))[2].trim());    		
    	}
    	*/
    }
    
    /***
     * Wrapper for database login
     * @throws SQLException
     */
    public void login() throws SQLException {
    	if (cust1!=null) cust1.login();
    	if (main1!=null) main1.login();
    	logger.debug("Logged in to all database");
    }

    /***
     * Wrapper for database logout
     * @throws SQLException
     */
    public void logout() throws SQLException {
    	if (cust1!=null) cust1.logout();
    	if (main1!=null) main1.logout();
    	logger.debug("Logged out from all database");
    }
    
    /***
     * Wrapper for database commit
     * @throws SQLException
     */
    public void commit() throws SQLException {
    	if (cust1!=null) cust1.commit();
    	if (main1!=null) main1.commit();
    	logger.debug("Committed to all database");
    }
    
    /***
     * Calls $ABPOPS_SHL/abp_dbconnection.ksh where decrypts user/password for db connection
     * @param user (the naming convention used in $ABPOPS_CFG/dbconnect_definition.cfg. eg: C1CUSTDB)
     * @return returns username/password string
     */
    public String getEncryptedUserPass(String user) throws IOException, InterruptedException {
		String path = System.getenv("ABPOPS_SHL");
		logger.debug("Getting credentials using shell script (" + path + "/dbconnect_definition.cfg)for user: " + user.toUpperCase());
		Process proc = Runtime.getRuntime().exec(path+"/abp_dbconnection.ksh " + user.toUpperCase());
		BufferedReader read = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		try {
			proc.waitFor();
			return read.readLine();
		}
		finally { read.close(); }
    }
    
	/***
	 * To get list of files to process
	 * @param path of working directory
	 * @return list of files
	 */
	public ArrayList<SmbFile> getSmbFiles(String path) throws MalformedURLException, SmbException, IOException {
		SmbFile folder = new SmbFile("smb://"+path,smbAuth);
		SmbFile[] listOfFiles = folder.listFiles();
		ArrayList<SmbFile> validFiles = new ArrayList<SmbFile>();

	    for (int i = 0; i < listOfFiles.length; i++) {
	    	if ((listOfFiles[i].isFile()) && (listOfFiles[i].getName().endsWith(".dat"))) {
	    	  validFiles.add(listOfFiles[i]);
	    	}
	    }
	    return validFiles;
	}
	
	/***
	 * Reads line from file and process line by line
	 * @param f input file
	 * @throws IOException
	 * @throws ParseException 
	 */
	public void readFile(SmbFile infile) throws IOException {
		// get f file name, change to filename with .done suffix
		// then open a new file to print output
		String newFile = "";
    	int pos = infile.getCanonicalPath().lastIndexOf(".");
    	int fpos = infile.getCanonicalPath().lastIndexOf("/");
    	if (pos > 0 ) {
    		newFile = p.getProperty("doneDir").trim()+File.separator+((infile.getCanonicalPath()).substring(fpos+1, pos))+".done"; 
    	} else throw new IOException("Unable to get .dat suffix file");

    	File f = new File(newFile);
    	if ( null != f.getParentFile() ) {
    		f.getParentFile().mkdir();
    	}
    	
    	logger.debug("Writing to " + newFile);
    	writer = new PrintWriter(new FileWriter(newFile));

    	//SmbFile somehow not working, check later
    	//SmbFile outfile = new SmbFile(newFile, smbAuth);
		//PrintWriter writer = new PrintWriter(new SmbFileOutputStream(outfile));

    	// open to-patch file, and reads line
		reader = new BufferedReader(new InputStreamReader(new SmbFileInputStream(infile)));
		logger.debug("Opened file to read : " + infile.getCanonicalPath());
		
		String line = "";
		while ((line=reader.readLine())!=null) {
			String newline = line;
			String[] parsed = line.split(",",-1);
			Offer offer = new Offer();
			
			logger.debug("Processing: " + line);
			// only needs to work when RECORDTYPE = OFFER
			if (parsed[0].equals("OFFERS")) {
				offer.setRecordType("OFFERS");
				offer.setAction(parsed[1].trim());
				offer.setOfferInstId(parsed[2].trim());

				// checks record if all arguments provided are sufficient. only active_dt needs input and or when action=updnul
				try {
					if (offer.getAction().equals("UPDACT")&&(parsed.length==5)) {
						if (parsed[3].trim().isEmpty()) throw new ParseException("ACTIVE_DT field cannot be null",3);
						if (parsed[4].trim().isEmpty()) throw new ParseException("NEW ACTIVE_DT field cannot be null",4);
						
						offer.setActiveDt(parsed[3].trim());
						offer.setNActiveDt(parsed[4].trim());
					}
					else if (offer.getAction().equals("UPDEND")&&(parsed.length==5)) {					
						offer.setInactiveDt(parsed[3].trim());
						offer.setNInactiveDt(parsed[4].trim());
					}
					else if (offer.getAction().equals("UPDALL")&&(parsed.length==7)) {
						offer.setActiveDt(parsed[3].trim());
						offer.setInactiveDt(parsed[4].trim());
						offer.setNActiveDt(parsed[5].trim());
						offer.setNInactiveDt(parsed[6].trim());
					}
					else if (offer.getAction().equals("CANCEL")&&(parsed.length==4)) {
						offer.setInactiveDt(parsed[3].trim());
					}
					else if (offer.getAction().equals("UPDNUL")&&(parsed.length==4)) {
						if (parsed[3].trim().isEmpty()) throw new ParseException("INACTIVE_DT field cannot be null",3);
						offer.setInactiveDt(parsed[3].trim());
					}
					else {
						newline = newline + ",  FAIL: INCORRECT ROW ENTRY";
					}
					
					// check Offer
					compute(offer);
					logger.debug(offer.getOfferInstId()+" | Computed\t: " + printClass(offer));					
					validateOffer(offer);
					logger.debug(offer.getOfferInstId()+" | Validated\t: " + printClass(offer));
					cust1.clearBatch();
					main1.clearBatch();

					if (!offer.getStatus().equals("null")) {
						newline = newline + ", FAIL: " + offer.getStatus();
						logger.debug(offer.getOfferInstId() + " | Error record: " + offer.getStatus());
					} else {
						// generate SQL Code						
						if (offer.getRemarks().equals("BTPP")) {
							cust1.addBatch(genCUSTBtppSQL(offer));
						}

						if (offer.getRemarks().matches("NEW_RC|NEW_RC_UNBILL_NRC")) {
							cust1.addBatch(genCUSTNewRcTermInstSQL(offer));
						}

						if (offer.getRemarks().matches("OLD_RC|OLD_RC_UNBILL_NRC")) {
							cust1.addBatch(genCUSTOldRcTermInstSQL(offer));
						}

						if ((offer.getAction().equals("CANCEL")) && (offer.getRemarks().matches("OLD_RC_UNBILL_NRC|NEW_RC_UNBILL_NRC|UNBILL_NRC"))) {
							cust1.addBatch(genCUSTChargeUnbilledSQL(offer));
							cust1.addBatch(genCUSTNrcSQL(offer));
						}

						// special case when UPDALL NEW ACTIVE_DT = NEW INACTIVE_DT (CANCEL)
						if ((offer.getAction().equals("UPDALL")) && (offer.getRemarks().equals("NEW_RC_UNBILL_NRC"))) {
							cust1.addBatch(genCUSTChargeUnbilledSQL(offer));
							cust1.addBatch(genCUSTNrcSQL(offer));							
						}

						cust1.addBatch(genCUSTOfferInstViewSQL(offer));
						main1.addBatch(genMAINOfferInstSQL(offer));
						
						int[] custrs = cust1.executeBatch();
						int[] mainrs = main1.executeBatch();
						int custsum = 0;
						int mainsum = 0;
						for (int i = 0;  i < custrs.length; i++) { custsum = custsum + custrs[i]; }
						for (int i = 0; i < mainrs.length; i++) { mainsum = mainsum + mainrs[i]; }

						newline = newline + ", SUCCESS";
					}
				} catch (ParseException e) {
					logger.warn(offer.getOfferInstId() + " | Parsing error: " + e.getMessage());
					logger.debug(e);
					newline = newline + ", FAIL: " + e.getMessage().replace("\n", " | ");
				} catch (SQLException e) {
					logger.warn(offer.getOfferInstId() + " | SQL error: " + e.getMessage());
					logger.debug(e);
					newline = newline + ", FAIL: " + e.getMessage().replace("\n", " | ");
				} catch (Exception e) {
					logger.warn(offer.getOfferInstId() + " | Error: " + e);
					logger.debug(e);
					newline = newline + ", FAIL: " + e.getMessage().replace("\n", " | ");
				}
			} else newline = newline + ",  FAIL: RECORD IS NOT FOR OFFER TYPE";

			// print every line read
			//if (newline.equals(line)) writer.println((newline+",SUCCESS").trim()); 
			//else writer.println((newline).trim());
			writer.println(newline);
		}
		
		reader.close();
		writer.close();
		
		// delete only if commit is true
		if (enable_commit=="Y")
			infile.delete();
	}
	
	/***
	 * Converts text string to date (packages yyyymmddhh24miss and yyyymmdd)
	 * @param s input date in string
	 * @return date or null
	 * @throws ParseException
	 */
	public Date stringToDate(String s) throws ParseException {
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		if (s.length()>8)
			return df.parse(s);
		else if (s.length()==8) {
			df = new SimpleDateFormat("yyyyMMdd");
			return df.parse(s);
		} else if (s.isEmpty()) {
			return null;
		}

		return null;
	}
	
	public String dateToString(Date d) throws ParseException {
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		return df.format(d);
	}
	
	/**
	 * Validate the offer instance remarks and dates 
	 */
	public void validateOffer(Offer offer) throws ParseException {

		// if offer inst id cannot be found
		if (offer.getOfferType().equals("null")) { offer.setStatus("OfferInstId is not found"); return; }

		// if returned remarks is cannot be patched
		if (offer.getRemarks().startsWith("CANT PATCH")) { offer.setStatus(offer.getRemarks());	return; }

		// if specific action is not allowed in certain scenarios
		if ((offer.getAction().matches("UPDACT|UPDALL")) && (!offer.getRemarks().matches("BTPP|NEW_RC|NEW_RC_UNBILL_NRC"))) {
			offer.setStatus(offer.getAction() + " is not allowed for " + offer.getRemarks());
			return;
		}
		
		// if NEW INACTIVE DT passed in is null, only UPDEND|UPDALL will pass in NEW INACTIVE DT
		if ((offer.getAction().matches("UPDEND|UPDALL")) 
				&& (offer.getNInactiveDt().equals("null")) 
				&& (!offer.getSvcEndDt().equals("null"))) {
			offer.setStatus("Cannot update NEW INACTIVE DT when Subscriber is terminated");
			return;
		}

		// UPDNUL cannot be processed for anything other than BTPP remarks
		if ((offer.getAction().equals("UPDNUL")) 
				&& (!offer.getRemarks().equals("BTPP"))) {
			offer.setStatus(offer.getAction() + " is not allowed for " + offer.getRemarks());
			return;
		}
		
		// UPDNUL fails when Subscriber is already terminated
		if ((offer.getAction().equals("UPDNUL")) 
				&& (offer.getInactiveDt().equals("null")) 
				&& (!offer.getSvcEndDt().equals("null"))) {
			offer.setStatus("Cannot update INACTIVE DT to NULL when Subscriber is terminated");
			return;			
		}

		// for UPDALL, if NEW ACTIVE DT & NEW INACTIVE DT results in CANCEL
		if ((offer.getAction().equals("UPDALL")) 
				&& (dateStringCompare(offer.getNInactiveDt(), offer.getNActiveDt())==0)) {
			if (offer.getRemarks().matches("BTPP|NEW_RC|NEW_RC_UNBILL_NRC")) {
				// can update, but cannot set ACTION=CANCEL as NEW ACTIVE DT and NEW INACTIVE DT might be different from original ACTIVE_DT, INACTIVE_DT
				// dont return, proceed with other checks
			} else {
				offer.setStatus("UPDALL is not allowed to cancel for " + offer.getRemarks());
				return;
			}
		}
		
		if ((offer.getAction().equals("UPDALL")) && (dateStringCompare(offer.getNInactiveDt(), offer.getNActiveDt()) < 0)) {
			offer.setStatus("NEW inactive_dt is earlier than NEW active_dt");
			return; 
		}

		if (offer.getAction().equals("UPDACT")) {
			if ((!offer.getInactiveDt().equals("null")) && (dateStringCompare(offer.getInactiveDt(), offer.getNActiveDt()) < 0)) {
				offer.setStatus("NEW ACTIVE DT is later than current offer INACTIVE_DT " + offer.getInactiveDt());
				return;
			} else if ((offer.getOfferType().equals("AO")) && (dateStringCompare(offer.getNActiveDt(), offer.getAccStartDt()) < 0)) {
				offer.setStatus("NEW ACTIVE DT is earlier than Account Active Dt " + offer.getAccStartDt());
				return;
			} else if ((offer.getOfferType().equals("SO")) && (dateStringCompare(offer.getNActiveDt(), offer.getSvcStartDt()) < 0)) {
				offer.setStatus("NEW ACTIVE DT is earlier than Subscriber Active Dt " + offer.getSvcStartDt());
				return;
			}
		}
		
		// for UPDEND, if NEW INACTIVE DT results in CANCEL
		if ((offer.getAction().equals("UPDEND")) && (!offer.getNInactiveDt().equals("null"))) {
			if (dateStringCompare(offer.getNInactiveDt(), offer.getActiveDt())==0) {
				offer.setAction("CANCEL");
				validateOffer(offer);
				return;
			} else if (dateStringCompare(offer.getNInactiveDt(), offer.getActiveDt()) < 0) {
				offer.setStatus("NEW inactive_dt is earlier than active_dt");
				return;
			} else if ((offer.getOfferType().equals("AO")) && (dateStringCompare(offer.getNInactiveDt(), offer.getAccStartDt()) < 0)) {
				offer.setStatus("NEW INACTIVE DT is earlier than Account Active Dt " + offer.getAccStartDt());
				return;
			} else if ((offer.getOfferType().equals("SO")) && (dateStringCompare(offer.getNInactiveDt(), offer.getSvcStartDt()) < 0)) {
				offer.setStatus("NEW INACTIVE DT is earlier than Subscriber Active Dt " + offer.getSvcStartDt());
				return;
			} else if ((!offer.getSvcEndDt().equals("null")) && ((offer.getOfferType().equals("SO")) && (dateStringCompare(offer.getSvcEndDt(), offer.getNInactiveDt()) < 0))) {
				offer.setStatus("NEW INACTIVE DT is later than Subscriber Termination Dt " + offer.getSvcEndDt());
				return;
			}
		}
		
		// for UPDEND, if NEW INACTIVE DT is null
		if ((offer.getAction().equals("UPDEND")) && (offer.getNInactiveDt().equals("null"))) {
			offer.setAction("UPDNUL");
			validateOffer(offer);
			return;
		}
		
		if (!offer.getNActiveDt().equals("null")) {
			if ((!offer.getAccStartDt().equals("null")) && (dateStringCompare(offer.getNActiveDt(), offer.getAccStartDt()) < 0)) {
				offer.setStatus("NEW ACTIVE DT is earlier than Account Start Dt");
				return;
			}
			
			if ((!offer.getSvcStartDt().equals("null")) && (dateStringCompare(offer.getActiveDt(), offer.getSvcStartDt()) < 0)) {
				offer.setStatus("NEW ACTIVE DT is earlier than Service Start Dt");
				return;
			}
		}
		
		if (!offer.getNInactiveDt().equals("null")) {
			if (dateStringCompare(offer.getNInactiveDt(), offer.getActiveDt()) < 0) {
				offer.setStatus("NEW INACTIVE DT is earlier than Offer ACTIVE DT");
				return;
			}
			
			if ((!offer.getAccStartDt().equals("null")) && (dateStringCompare(offer.getNInactiveDt(), offer.getAccStartDt()) < 0)) {
				offer.setStatus("NEW INACTIVE DT is earlier than Account Start Dt");
				return;
			}
			
			if ((!offer.getSvcStartDt().equals("null")) && (dateStringCompare(offer.getNInactiveDt(), offer.getSvcStartDt()) < 0)) {
				offer.setStatus("NEW INACTIVE DT is earlier than Service Start Dt");
				return;
			}
		}
	}
	
	public int dateStringCompare(String s1, String s2) throws ParseException {
		DateFormat df; 
	
		df = (s1.length()>8) ? new SimpleDateFormat("yyyyMMddHHmmss") : new SimpleDateFormat("yyyyMMdd"); 
		Date d1 = df.parse(s1);
		
		df = (s2.length()>8) ? new SimpleDateFormat("yyyyMMddHHmmss") : new SimpleDateFormat("yyyyMMdd");
		Date d2 = df.parse(s2);
		
		return d1.compareTo(d2);
	}
	
	// function to print all declared variables in the object
	public String printClass(Object obj) {
		StringBuilder sb = new StringBuilder();
		try {
	    	Class<?> tmpClass = obj.getClass();	    
	    	for (Field f : tmpClass.getDeclaredFields()) {
	    		f.setAccessible(true);
	    		String name = f.getName();
	    		Object value = f.get(obj);

	    		if (!name.equals("this$0")) {
	    			sb.append(name+":="+value+", ");
	    		}
	    	}
	    	return sb.toString();
		} catch (Exception e) {
			logger.error(e);
			return "ERROR: " + e.getMessage().replace("\n", " | ");
		}
	}
	
  /**
     * Checks for offer_inst_id status
     * If errors if returns null for offer_type
  */
	public void compute(Offer offer) throws SQLException {
		ResultSet rs = null;
		StringBuilder sqlString = new StringBuilder();

		//String sql = "with nrclist as (select offer_inst_id from nrc_term_inst@cust1 where offer_inst_id = 252023043) select o.offer_inst_id from offeR_inst_view@cust1 o, nrclist n  where o.offer_inst_id = n.offer_inst_id";
		sqlString.append("WITH NRCLIST AS ");
		sqlString.append("(SELECT NTI.OFFER_INST_ID ");
		sqlString.append(" , Max(NTI.NRC_TERM_INST_ID) NTI ");
		sqlString.append(" , Max(Nvl(NRC.NO_BILL,NTI.NO_BILL)) NRC_CXL ");
		sqlString.append(" , MAX((SELECT DECODE(COUNT(1),0,'BILLED','UNBILL') FROM CBS_OWNER.CHARGE_UNBILLED ");
		sqlString.append("        WHERE CHARGE_TYPE = 2 AND CHARGE_ID1 = NTI.NRC_TERM_INST_ID)) NRC_STAT ");
		sqlString.append(" , Count(1) NRC_CNT ");
		sqlString.append(" FROM CBS_OWNER.NRC_TERM_INST NTI, CBS_OWNER.NRC ");
		sqlString.append(" WHERE NTI.NRC_TERM_INST_ID = NRC.NRC_TERM_INST_ID ");
		sqlString.append(" GROUP BY NTI.OFFER_INST_ID ");
		sqlString.append(") ");
		sqlString.append(",OCM AS ( ");
		sqlString.append("  SELECT OFFER_ID, CORRIDOR_PLAN_ID, RESELLER_VERSION_ID RVID FROM CBS_OWNER.OFFER_CORRIDOR_MAP ");
		sqlString.append(") ");
		sqlString.append(",SUBS AS ( ");
		sqlString.append("  SELECT SUBSCR_NO, SUBSCR_NO_RESETS,SERVICE_ACTIVE_DT,SERVICE_INACTIVE_DT FROM CBS_OWNER.SUBSCRIBER_VIEW WHERE VIEW_STATUS=2 ");
		sqlString.append(") ");
		sqlString.append(", OD AS ( ");
		sqlString.append("  SELECT OIV.OFFER_INST_ID ");
		sqlString.append("  , DECODE(ORF.OFFER_TYPE, 1,'AO',2,'PO',3,'SO') OFFER_TYPE ");
		sqlString.append("  , OV.PRIMARY_LIST_PRICE OFFER_SUBTYPE ");
		sqlString.append("  , NVL(OIV.PARENT_ACCOUNT_NO,NVL(OIV.ACCOUNT_NO,OIV.GUIDED_ACCOUNT_NO)) ACCNO ");
		sqlString.append("  , (SELECT to_char(DATE_ACTIVE,'yyyymmddhh24miss') FROM CBS_OWNER.CMF ");
		sqlString.append("     WHERE ACCOUNT_NO = OIV.PARENT_ACCOUNT_NO OR ACCOUNT_NO = OIV.ACCOUNT_NO OR ACCOUNT_NO = OIV.GUIDED_ACCOUNT_NO ");
		sqlString.append("  ) ACCT_START ");
		sqlString.append("  , to_char(SUBS.SERVICE_ACTIVE_DT,'yyyymmddhh24miss') SVC_START ");
		sqlString.append("  , to_char(SUBS.SERVICE_INACTIVE_DT,'yyyymmddhh24miss') SVC_END ");
		sqlString.append("  , OIV.SUBSCR_NO ");
		sqlString.append("  , OIV.SUBSCR_NO_RESETS ");
		sqlString.append("  , OV.OFFER_ID ");
		sqlString.append("  , to_char(OIV.ACTIVE_DT,'yyyymmddhh24miss') ACTIVE_DT, to_char(OIV.INACTIVE_DT,'yyyymmddhh24miss') INACTIVE_DT");
		sqlString.append("  , (SELECT COUNT(1) FROM CBS_OWNER.BT_PROMOTION_PLAN_INST BPI ");
		sqlString.append("     WHERE BPI.OFFER_INST_ID= OIV.OFFER_INST_ID ");
		sqlString.append("  ) BTPP ");
		sqlString.append("  , OCM.CORRIDOR_PLAN_ID CORRIDOR ");
		sqlString.append("  , RTI.PROCESSING_STATUS RC_PROCESS ");
		sqlString.append("  , RTI.STATUS RC_STAT ");
		sqlString.append("  , N.NRC_CXL ");
		sqlString.append("  , N.NRC_STAT ");
		sqlString.append("  , N.NTI ");
		sqlString.append("  , N.NRC_CNT ");
		sqlString.append("  FROM CBS_OWNER.OFFER_INST_VIEW OIV, CBS_OWNER.OFFER_VALUES OV, CBS_OWNER.OFFER_REF ORF, CBS_OWNER.RC_TERM_INST RTI, NRCLIST N, OCM, SUBS ");
		sqlString.append("  WHERE OIV.OFFER_ID =OV.OFFER_ID ");
		sqlString.append("  AND OIV.OFFER_ID = ORF.OFFER_ID AND OV.RESELLER_VERSION_ID = ORF.RESELLER_VERSION_ID ");
		sqlString.append("  AND OIV.VIEW_STATUS = 2 ");
		sqlString.append("  AND OV.RESELLER_VERSION_ID IN (SELECT MAX(RESELLER_VERSION_ID) FROM CBS_OWNER.RESELLER_VERSION WHERE INACTIVE_DATE IS NULL ) ");
		sqlString.append("  AND OIV.OFFER_INST_ID = RTI.OFFER_INST_ID(+) ");
		sqlString.append("  AND OIV.OFFER_INST_ID = N.OFFER_INST_ID(+) ");
		sqlString.append("  AND OV.OFFER_ID=OCM.OFFER_ID(+) ");
		sqlString.append("  AND OV.RESELLER_VERSION_ID=OCM.RVID(+) ");
		sqlString.append("  AND OIV.SUBSCR_NO=SUBS.SUBSCR_NO(+) ");
		sqlString.append("  AND OIV.SUBSCR_NO_RESETS=SUBS.SUBSCR_NO_RESETS(+) ");
		sqlString.append("  AND OIV.OFFER_INST_ID IN (");
		sqlString.append(offer.getOfferInstId());
		sqlString.append(") ");
		sqlString.append(") ");
		sqlString.append("SELECT ");
		sqlString.append("  CASE ");
		sqlString.append("    WHEN OFFER_TYPE = 'PO' THEN 'CANT PATCH PO' ");
		sqlString.append("    WHEN CORRIDOR IS NOT NULL THEN 'CANT PATCH CORRIDOR' ");
		sqlString.append("    WHEN NRC_CNT>1 THEN 'CANT PATCH MULTIPLE NRCS' ");
		sqlString.append("    WHEN (RC_PROCESS=0 AND RC_STAT=2 AND NRC_CXL=1) THEN 'NEW_RC' ");
		sqlString.append("    WHEN (RC_PROCESS=0 AND RC_STAT=2 AND NRC_STAT='BILLED') THEN 'NEW_RC' ");
		sqlString.append("    WHEN (RC_PROCESS=0 AND RC_STAT=2 AND NRC_STAT='UNBILL') THEN 'NEW_RC_UNBILL_NRC' ");
		sqlString.append("    WHEN (RC_PROCESS=0 AND RC_STAT=2 AND NTI IS NULL) THEN 'NEW_RC' ");
		sqlString.append("    WHEN (RC_STAT IS NOT null AND NRC_CXL=1) THEN 'OLD_RC' ");
		sqlString.append("    WHEN (RC_STAT IS NOT null AND NRC_STAT='BILLED') THEN 'OLD_RC' ");
		sqlString.append("    WHEN (RC_STAT IS NOT NULL AND NRC_STAT='UNBILL') THEN 'OLD_RC_UNBILL_NRC' ");
		sqlString.append("    WHEN (RC_STAT IS NOT NULL AND NTI IS NULL) THEN 'OLD_RC' ");
		sqlString.append("    WHEN (RC_STAT IS NULL AND NRC_CXL=1) THEN 'NOBILL_NRC' ");
		sqlString.append("    WHEN (RC_STAT IS NULL AND NRC_STAT='BILLED') THEN 'BILLED_NRC' ");
		sqlString.append("    WHEN (RC_STAT IS NULL AND NRC_STAT='UNBILL') THEN 'UNBILL_NRC' ");
		sqlString.append("    WHEN BTPP <>0 THEN 'BTPP' ");
		sqlString.append("    WHEN OFFER_SUBTYPE IN ('SERVC','EQUIP','PRCON') THEN 'CONTRACT' ");
		sqlString.append("    ELSE 'EMPTY_OFFER' ");
		sqlString.append("  END REMARKS ");
		sqlString.append(", OD.OFFER_INST_ID, OD.OFFER_TYPE, OD.OFFER_SUBTYPE, OD.ACCNO, OD.ACCT_START ");
		sqlString.append(", OD.SVC_START, OD.SVC_END, OD.SUBSCR_NO, OD.SUBSCR_NO_RESETS, OD.OFFER_ID, OD.ACTIVE_DT, OD.INACTIVE_DT ");
		sqlString.append(", OD.BTPP, OD.CORRIDOR, OD.RC_PROCESS, OD.RC_STAT, OD.NRC_CXL, OD.NRC_STAT, OD.NTI, OD.NRC_CNT ");
		sqlString.append("FROM OD ");

		rs = cust1.executeQuery(sqlString.toString());
		logger.debug("SQL::"+offer.getOfferInstId()+"::"+sqlString.toString());
		
		while (rs.next()) {
			offer.setRemarks(rs.getString("REMARKS"));
			offer.setActiveDt(rs.getString("ACTIVE_DT"));
			offer.setInactiveDt(rs.getString("INACTIVE_DT"));			
			offer.setOfferType(rs.getString("OFFER_TYPE"));
			offer.setAccStartDt(rs.getString("ACCT_START"));
			offer.setSvcStartDt(rs.getString("SVC_START"));
			offer.setSvcEndDt(rs.getString("SVC_END"));
			offer.setNTI(rs.getString("NTI"));
		}
    }

	public int dayCompare(Date date1, Date date2) throws ParseException {
		long diff = (date1.getTime() - date2.getTime())/(24*60*60*1000);
		return (int) diff;
	}

	/***
	 * Offer Updates for CUST DB
	 * @param action - the type of action of the update
	 * @return - returns sql string
	 */
   public String genCUSTOfferInstViewSQL(Offer offer) throws ParseException {
	   DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
	   // select statement
	   String activedt = offer.getActiveDt().equals("null") ? " and active_dt is null" : " and active_dt = to_date('" + offer.getActiveDt() + (offer.getActiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   String inactivedt = offer.getInactiveDt().equals("null") ? " and inactive_dt is null": " and inactive_dt = to_date('" + offer.getInactiveDt() + (offer.getInactiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   // update statement
	   String newactivedt = offer.getNActiveDt().equals("null") ? " active_dt = null" : " active_dt = to_date('" + offer.getNActiveDt() + (offer.getNActiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   String newinactivedt = offer.getNInactiveDt().equals("null") ? " inactive_dt = null" : " inactive_dt = to_date('" + offer.getNInactiveDt() + (offer.getNInactiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   
	   StringBuilder sb = new StringBuilder();
	   														sb.append("UPDATE CBS_OWNER.OFFER_INST_VIEW SET ");
	   if (offer.getAction().matches("UPDACT|UPDALL")) 		sb.append(newactivedt);
	   if (offer.getAction().matches("UPDALL")) 			sb.append(" ,");
	   if (offer.getAction().matches("UPDEND|UPDALL")) 		sb.append(newinactivedt);

	   if (!offer.getNInactiveDt().equals("null")) {
			if ((offer.getAction().matches("UPDALL"))
			&& (dateStringCompare(offer.getNInactiveDt(),offer.getNActiveDt())==0)) sb.append(" , disconnect_reason = 1");
			else if ((offer.getAction().matches("UPDEND|UPDALL")) 
			&& (dayCompare(df.parse(offer.getNInactiveDt()),new Date())>=2))		sb.append(" , disconnect_reason = NULL");
			else if ((offer.getAction().matches("UPDEND|UPDALL")) 
			&& (dayCompare(df.parse(offer.getNInactiveDt()),new Date())<2))			sb.append(" , disconnect_reason = 1");
	   }
	   
	   if (offer.getAction().matches("CANCEL")) 			sb.append(" inactive_dt = active_dt, disconnect_reason = 1");
	   if (offer.getAction().matches("UPDNUL")) 			sb.append(" inactive_dt = NULL, disconnect_reason = NULL");
	   														sb.append(" where view_status = 2 and offer_inst_id = " + offer.getOfferInstId());
	   if (offer.getAction().matches("UPDACT|UPDALL"))		sb.append(activedt);
	   if (!(offer.getAction().matches("UPDACT")))			sb.append(inactivedt);
	   
	   logger.info("SQL::"+offer.getOfferInstId()+"::"+ sb.toString());
	   return sb.toString();
   }
   
   public String genMAINOfferInstSQL(Offer offer) throws ParseException {
	   DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
	   // select statement
	   String activedt = offer.getActiveDt().equals("null") ? " and active_dt is null" : " and active_dt = to_date('" + offer.getActiveDt() + (offer.getActiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   String inactivedt = offer.getInactiveDt().equals("null") ? " and inactive_dt is null": " and inactive_dt = to_date('" + offer.getInactiveDt() + (offer.getInactiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   // update statement
	   String newactivedt = offer.getNActiveDt().equals("null") ? " active_dt = null" : " active_dt = to_date('" + offer.getNActiveDt() + (offer.getNActiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   String newinactivedt = offer.getNInactiveDt().equals("null") ? " inactive_dt = null" : " inactive_dt = to_date('" + offer.getNInactiveDt() + (offer.getNInactiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	    
	   StringBuilder sb = new StringBuilder();
	   														sb.append("UPDATE CBS_OWNER.OFFER_INST SET ");
	   if (offer.getAction().matches("UPDACT|UPDALL")) 		sb.append(newactivedt);
	   if (offer.getAction().matches("UPDALL")) 			sb.append(" ,");
	   if (offer.getAction().matches("UPDEND|UPDALL")) 		sb.append(newinactivedt);

	   if (offer.getNInactiveDt()!=null) {
			if ((offer.getAction().matches("UPDALL"))
			&& (dateStringCompare(offer.getNInactiveDt(),offer.getNActiveDt())==0)) sb.append(" , disconnect_reason = 1");
			else if ((offer.getAction().matches("UPDEND|UPDALL")) 
			&& (dayCompare(df.parse(offer.getNInactiveDt()),new Date())>=2))		sb.append(" , disconnect_reason = NULL");
			else if ((offer.getAction().matches("UPDEND|UPDALL")) 
			&& (dayCompare(df.parse(offer.getNInactiveDt()),new Date())<2))			sb.append(" , disconnect_reason = 1");
	   }
	   
	   if (offer.getAction().matches("CANCEL")) 			sb.append(" inactive_dt = active_dt, disconnect_reason = 1");
	   if (offer.getAction().matches("UPDNUL")) 			sb.append(" inactive_dt = NULL, disconnect_reason = NULL");
	   														sb.append(" where offer_inst_id = " + offer.getOfferInstId());
	   if (offer.getAction().matches("UPDACT|UPDALL"))		sb.append(activedt);
	   if (!(offer.getAction().matches("UPDACT")))			sb.append(inactivedt);

	   logger.info("SQL::"+offer.getOfferInstId()+"::"+ sb.toString());
	   return sb.toString();
   }
   
   public String genCUSTChargeUnbilledSQL(Offer offer) {
	   StringBuilder sb = new StringBuilder();
	   sb.append("UPDATE cbs_owner.charge_unbilled set no_bill=1");
	   sb.append("WHERE charge_type = 2 and charge_id2 = 0 and charge_id3 = 0 and charge_id1 = " + offer.getNTI());

	   logger.info("SQL::"+offer.getOfferInstId()+"::"+ sb.toString());
	   return sb.toString();
   }

   public String genCUSTNrcSQL(Offer offer) {
	   StringBuilder sb = new StringBuilder();
	   sb.append("UPDATE cbs_owner.nrc set no_bill = 1 where nrc_term_inst_id = " + offer.getNTI());

	   logger.info("SQL::"+offer.getOfferInstId()+"::"+ sb.toString());
	   return sb.toString();
   }
   
   public String genCUSTBtppSQL(Offer offer) {
	   // update statement
	   String newactivedt = offer.getNActiveDt().equals("null") ? " active_dt = null" : " active_dt = to_date('" + offer.getNActiveDt() + (offer.getNActiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   String newinactivedt = offer.getNInactiveDt().equals("null") ? " inactive_dt = null" : " inactive_dt = to_date('" + offer.getNInactiveDt() + (offer.getNInactiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");

	   StringBuilder sb = new StringBuilder();
	   														sb.append("UPDATE cbs_owner.bt_promotion_plan_inst SET");
	   if (offer.getAction().matches("UPDACT|UPDALL"))		sb.append( newactivedt );
	   if (offer.getAction().matches("UPDALL"))				sb.append(" ,");
	   if (offer.getAction().matches("UPDEND|UPDALL"))		sb.append( newinactivedt );
	   if (offer.getAction().matches("CANCEL"))				sb.append(" inactive_dt = active_dt");
	   if (offer.getAction().matches("UPDNUL"))				sb.append(" inactive_dt = NULL");
	   														sb.append(" WHERE offer_inst_id = " + offer.getOfferInstId());

	   logger.info("SQL::"+offer.getOfferInstId()+"::"+ sb.toString());
	   return sb.toString();
   }
   
   public String genCUSTNewRcTermInstSQL(Offer offer) {
	   // update statement
	   String newactivedt = offer.getNActiveDt().equals("null") ? " rc_term_inst_active_dt = null" : " rc_term_inst_active_dt = to_date('" + offer.getNActiveDt() + (offer.getNActiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	   String newinactivedt = offer.getNInactiveDt().equals("null") ? " rc_term_inst_inactive_dt = null" : " rc_term_inst_inactive_dt = to_date('" + offer.getNInactiveDt() + (offer.getNInactiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	 
	   StringBuilder sb = new StringBuilder();
	   														sb.append("UPDATE CBS_OWNER.RC_TERM_INST SET");
	   if (offer.getAction().matches("UPDACT|UPDALL"))		sb.append( newactivedt );
	   if (offer.getAction().matches("UPDALL"))				sb.append(" ,");
	   if (offer.getAction().matches("UPDEND|UPDALL"))		sb.append( newinactivedt + ", status = 3");
	   if (offer.getAction().matches("CANCEL"))				sb.append(" rc_term_inst_inactive_dt = rc_term_inst_active_dt, status = 3");
	   														sb.append(" WHERE status = 2 and processing_status = 0 and offer_inst_id = " + offer.getOfferInstId());

	   logger.info("SQL::"+offer.getOfferInstId()+"::"+ sb.toString());														
	   return sb.toString();
   }
   
   public String genCUSTOldRcTermInstSQL(Offer offer) {
	   // update statement
	   String newinactivedt = offer.getNInactiveDt().equals("null") ? " rc_term_inst_inactive_dt = null" : " rc_term_inst_inactive_dt = to_date('" + offer.getNInactiveDt() + (offer.getNInactiveDt().length() > 8 ? "','yyyymmddhh24miss')" : "','yyyymmdd')");
	 	   
	   StringBuilder sb = new StringBuilder();
	   														sb.append("UPDATE CBS_OWNER.RC_TERM_INST SET");
	   if (offer.getAction().matches("UPDEND"))				sb.append( newinactivedt + ", status = 3");
	   if (offer.getAction().matches("CANCEL"))				sb.append(" rc_term_inst_inactive_dt = rc_term_inst_active_dt, status = 3");
	   														sb.append(" WHERE offer_inst_id = " + offer.getOfferInstId());

	   logger.info("SQL::"+offer.getOfferInstId()+"::"+ sb.toString());														
	   return sb.toString();
   }

  /**
     * Log batch SQL error
     * @param b BatchUpdateException
     * @return Nothing
  */
   	public static void printBatchUpdateException(BatchUpdateException b) {
   		logger.error("----BatchUpdateException----");
//  	  logger.error("SQLState:  " + b.getSQLState());
		logger.error("Message:  " + b.getMessage());
//  	  logger.error("Vendor:  " + b.getErrorCode());
  	  	logger.error("Update counts:  ");
  	  	int [] updateCounts = b.getUpdateCounts();

  	  	for (int i = 0; i < updateCounts.length; i++) {
  	  		logger.error(updateCounts[i] + "   ");
  	  	}
  	}
}
