package charley.wu.flink.datahub294.async;

import com.aliyun.datahub.DatahubClient;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2019/5/15
 */
public interface ReadCallback {

  void execute(DatahubClient client, ReadContext context);
}
