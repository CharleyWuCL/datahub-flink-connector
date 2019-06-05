package charley.wu.flink.datahub.async;


import com.aliyun.datahub.client.DatahubClient;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/15
 */
public interface ReadCallback {

  void execute(DatahubClient client, ReadContext context, boolean cancelled);
}
