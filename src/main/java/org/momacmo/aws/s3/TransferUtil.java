package org.momacmo.aws.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * Utility routines for transferring objects to/from AWS S3 Storage
 * 
 * @author chuck
 *
 */
public class TransferUtil  {

	AmazonS3 s3;
	TransferManager tx;
	Logger log;
	Level level;
	long transferLength;
	long time;
	S3ProgressListener listener;
	long MIN_SIZE = 65535;

	public TransferUtil(AmazonS3 awsS3, Logger logger) {
		s3 = awsS3;
		tx = TransferManagerBuilder.standard().withS3Client(s3).build();
		log = logger;
		if (log == null)
			log = Logger.getGlobal();
		level = log.getLevel();
		if (level == null)
			level = Level.INFO;
		listener = new S3ProgressListener(1,100,10,log,"S3 Transfer");
	}

	public TransferUtil(AmazonS3 awsS3) {
		this(awsS3, null);
	}

	public TransferUtil(String awsCredentialProfile, String awsRegion, Logger logger ) {

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider(awsCredentialProfile).getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/Users/chuck/.aws/credentials), and is in valid format.", e);
		}

		s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(awsRegion).build();
		tx = TransferManagerBuilder.standard().withS3Client(s3).build();
		log = logger;
		if (log == null)
			log = Logger.getGlobal();
		level = log.getLevel();
		if (level == null)
			level = Level.INFO;
		listener = new S3ProgressListener(1,100,10,log,"S3 Transfer");
	}

	public TransferUtil(String awsRegion) {
		this("default", awsRegion, null );
	}
	
	public void close() {
		tx.shutdownNow(true);
	}
	
	public boolean bucketExists( String s3Bucket ) {
		return s3.doesBucketExistV2(s3Bucket);
	}
	
	public boolean objectExists( String s3Bucket, String s3ObjectName ) {
		return s3.doesObjectExist(s3Bucket,s3ObjectName);
	}
	
	public void setLogIncrements( float pctInc, float timeInc) {
		listener.setIncrements( pctInc, timeInc );
	}

	public ObjectMetadata download(String inputBucket, String inputKey, String outputPath) {

		log.log(level, "Download started for: S3 bucket: " + inputBucket + " name: " + inputKey + " to: " + outputPath);
		if (!s3.doesObjectExist(inputBucket, inputKey))
			throw new AmazonClientException("S3 bucket: " + inputBucket + " key: " + inputKey + " does not exist");
		File f = new File(outputPath);
		if (f.exists()) {
			if (f.canWrite())
				log.log(level, "*** Warning - will overwrite existing file: " + outputPath);
			else
				throw new AmazonClientException("Cannot write to file: " + outputPath);
		} else {
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
				throw new AmazonClientException("Could not create output file: " + outputPath);
			}
		}
		transferLength = s3.getObject(inputBucket,inputKey).getObjectMetadata().getContentLength();
		GetObjectRequest request = new GetObjectRequest(inputBucket, inputKey);
		Download download = tx.download(request, f);
		download.addProgressListener(listener);
		listener.reset(transferLength);
		try {
			download.waitForCompletion();
		} catch (Exception e) {
			e.printStackTrace();
			throw new AmazonClientException(
					"Transfer interrupted for S3 bucket: " + inputBucket + " name: " + inputKey + " to: " + outputPath);
		}
		log.log(level, "\nDownload Completed for: S3 bucket: " + inputBucket + " object name: " + inputKey
				+ " to file: " + outputPath);
		return download.getObjectMetadata();
	}
	
  public ObjectMetadata download(String inputBucket, String inputKey, long startByte, long endByte, String outputPath) {

    log.log(level, "Download started for: S3 bucket: " + inputBucket + " name: " + inputKey + " to: " + outputPath);
    if (!s3.doesObjectExist(inputBucket, inputKey))
      throw new AmazonClientException("S3 bucket: " + inputBucket + " key: " + inputKey + " does not exist");
    File f = new File(outputPath);
    if (f.exists()) {
      if (f.canWrite())
        log.log(level, "*** Warning - will overwrite existing file: " + outputPath);
      else
        throw new AmazonClientException("Cannot write to file: " + outputPath);
    } else {
      try {
        f.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
        throw new AmazonClientException("Could not create output file: " + outputPath);
      }
    }
    transferLength = s3.getObject(inputBucket,inputKey).getObjectMetadata().getContentLength();
    GetObjectRequest request = new GetObjectRequest(inputBucket, inputKey).withRange(startByte,endByte);
    Download download = tx.download(request, f);
    download.addProgressListener(listener);
    listener.reset(transferLength);
    try {
      download.waitForCompletion();
    } catch (Exception e) {
      e.printStackTrace();
      throw new AmazonClientException(
          "Transfer interrupted for S3 bucket: " + inputBucket + " name: " + inputKey + " to: " + outputPath);
    }
    log.log(level, "\nDownload Completed for: S3 bucket: " + inputBucket + " object name: " + inputKey
        + " to file: " + outputPath);
    return download.getObjectMetadata();
  }


	public void upload(String inputPath, String outputBucket, String outputKey) {

		log.log(level, "Upload started for path: " + inputPath + " to S3 bucket: " + outputBucket + " object name: "
				+ outputKey);

		ObjectMetadata omd = new ObjectMetadata();
		omd.setContentType("application/octet-stream");
		File f = new File(inputPath);
		InputStream is;
		try {
			is = new FileInputStream(f);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			System.out.println("*** Could not find input file " + inputPath + " ***");
			throw new AmazonClientException("*** Could not find input file " + inputPath + " ***");
		}
		omd.setContentLength(f.length());
		PutObjectRequest request = new PutObjectRequest(outputBucket, outputKey, is, omd);

		Upload upload = tx.upload(request);
		upload.addProgressListener(listener);
		listener.reset(f.length());
		try {
			upload.waitForCompletion();
		} catch (Exception e) {
			try {
				is.close();
			} catch (Exception ex) {
			}
			throw new AmazonClientException("Upload faild for path: " + inputPath + " to S3 bucket: " + outputBucket
					+ " object name: " + outputKey);
		}
		try {
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new AmazonClientException("*** Could not close file input stream ***");
		}

		log.log(level, "Upload complete for path: " + inputPath + " to S3 bucket: " + outputBucket + " object name: "
				+ outputKey);
	}

	public static void main(String[] args) {

		TransferUtil tx = new TransferUtil("default", "ca-central-1", Logger.getLogger("TestTransfer"));
		File ftmp0 = null;
		try {
			ftmp0 = File.createTempFile("hdfTemp", "h5");
			ftmp0.deleteOnExit();
		} catch (IOException e) {
			e.printStackTrace();
			throw new AmazonClientException("Could not create temp file");
		}
		tx.download("glacier-das", "oc2toOc15/sensor_2019-09-18T230005Z.h5", ftmp0.getAbsolutePath());
		tx.upload(ftmp0.getAbsolutePath(), "glacier-das", "TempInt16/sensor_2019-09-18T230005Z.int16");
		tx.close();
	}

	/**
	 * Creates a temporary file with text data to demonstrate uploading a file to
	 * Amazon S3
	 *
	 * @return A newly created temporary file with text data.
	 *
	 * @throws IOException
	 */
	private static File createSampleFile() throws IOException {
		File file = File.createTempFile("aws-java-sdk-", ".txt");
		file.deleteOnExit();
		FileOutputStream fos = new FileOutputStream(file);
		Writer writer = new OutputStreamWriter(fos);
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.write("01234567890112345678901234\n");
		writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
		writer.write("01234567890112345678901234\n");
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.close();
		System.out.println(" Data length: " + file.length());

		return file;
	}

}
