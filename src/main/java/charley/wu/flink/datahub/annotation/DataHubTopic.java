package charley.wu.flink.datahub.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DataHubTopic {

  String name();

  int shardNum();

  String comment();
}
