import com.google.protobuf.InvalidProtocolBufferException;
import filters.generated.FilterProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class HBTag {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost:2181");
        conf.set("zookeeper.znode.parent", "/hbase");

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("filter_test"));

        FilterList filterList = new FilterList(
                FilterList.Operator.MUST_PASS_ONE, Arrays.asList(new SimpleFilter()));
        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        while (it.hasNext()) {
            it.next();
        }
        Get get = new Get("row-1".getBytes());
        get = get.setFilter(new SimpleFilter());
        System.out.println("Fetching...");
        Result result = table.get(get);
        System.out.println("Fetched?");
        System.out.println("Val: " + new String(result.getValue("data".getBytes(), "message".getBytes())));
    }
}

class SimpleFilter extends FilterBase {
    @Override
    public Cell transformCell(Cell v) {
        System.out.println("\n\n\n\n\ntransforming....\n\n\n\n\n");

        if (v.getFamilyArray() == "data".getBytes() && v.getQualifierArray() == "message".getBytes()) {
            CellBuilder builder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
            return builder.setFamily(v.getFamilyArray()).setQualifier(v.getQualifierArray())
                    .setRow(v.getRowArray()).setValue("I have been transformed".getBytes()).build();
        }
        return v;
    }

    public boolean filterRow() throws IOException {
        return false;
    }

    @Override
    public byte [] toByteArray() {
        FilterProtos.SimpleFilter.Builder builder =
                FilterProtos.SimpleFilter.newBuilder();

        return builder.build().toByteArray();
    }

    //@Override
    public static Filter parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        FilterProtos.SimpleFilter proto;
        try {
            proto = FilterProtos.SimpleFilter.parseFrom(pbBytes); // co SimpleFilter-7-Read Used by the servers to establish the filter instance with the correct values.
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new SimpleFilter();
    }
}
