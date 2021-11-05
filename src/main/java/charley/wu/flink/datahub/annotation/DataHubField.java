package charley.wu.flink.datahub.annotation;

import com.aliyun.datahub.client.model.FieldType;
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
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DataHubField {

  String alias();

  FieldType type();

  String comment() default "";
}
