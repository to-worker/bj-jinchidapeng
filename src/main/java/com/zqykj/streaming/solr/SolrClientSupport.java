package com.zqykj.streaming.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.Logging;
import org.apache.spark.Logging$;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.*;

/**
 * @author feng.wei
 * @date 2018/5/21
 */
public class SolrClientSupport {

    private static Logger log = LoggerFactory.getLogger(com.zqykj.streaming.solr.SolrClient.class);

    private static Map<String, CloudSolrClient> solrServers = new HashMap<String, CloudSolrClient>();

    public static CloudSolrClient getSolrServer(String key, String zkChroot, String collection) {
        CloudSolrClient solr = null;
        //synchronized (solrServers) {
        //solr = solrServers.get(key);
        //if (solr == null) {
        solr = new CloudSolrClient.Builder().sendDirectUpdatesToShardLeadersOnly().withZkHost(key)
                .withZkChroot(zkChroot).build();
        solr.setZkClientTimeout(15 * 1000);
        solr.setParallelCacheRefreshes(12);
        solr.setDefaultCollection(collection);
        //solr.connect();
        //solrServers.put(key, solr);
        // }
        // }
        return solr;
    }

    public static void indexDocs(final String zkHost, final String zkChroot, final String collection,
            final int batchSize, JavaRDD<SolrInputDocument> docs) {

        docs.foreachPartition(new VoidFunction<Iterator<SolrInputDocument>>() {
            public void call(Iterator<SolrInputDocument> solrInputDocumentIterator) throws Exception {
                final CloudSolrClient solrServer = getSolrServer(zkHost, zkChroot, collection);
                List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>();
                Date indexedAt = new Date();
                while (solrInputDocumentIterator.hasNext()) {
                    SolrInputDocument inputDoc = solrInputDocumentIterator.next();
                    inputDoc.addField("_indexed_at_tdt", indexedAt);
                    batch.add(inputDoc);
                    if (batch.size() >= batchSize)
                        sendBatchToSolr(solrServer, collection, batch);
                }
                if (!batch.isEmpty()) {
                    sendBatchToSolr(solrServer, collection, batch);
                }

                solrServer.close();
            }
        });
    }

    public static void sendBatchToSolr(CloudSolrClient cloudSolrClient, String collection,
            Collection<SolrInputDocument> batch) {
        try {
            log.info("=======> write {} data to collection: {}", batch.size(), collection);
            cloudSolrClient.add(batch);
        } catch (Exception e) {
            if (shouldRetry(e)) {
                log.error("Send batch to collection " + collection + " failed due to " + e + "; will retry ...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e1) {
                    Thread.interrupted();
                }

                try {
                    cloudSolrClient.add(batch);
                } catch (Exception e1) {
                    log.error("Retry send batch to collection " + collection + " failed due to: " + e1, e1);
                }
            } else {
                log.error("Send batch to collection " + collection + " failed due to: " + e, e);
            }
        } finally {
//            try {
//                cloudSolrClient.commit(true, false);
//            } catch (Exception e) {
//                log.error("solr commit has exception: {}", e);
//            }
            batch.clear();
        }
    }

    private static boolean shouldRetry(Exception exc) {
        Throwable rootCause = SolrException.getRootCause(exc);
        return (rootCause instanceof ConnectException || rootCause instanceof SocketException);

    }

}
