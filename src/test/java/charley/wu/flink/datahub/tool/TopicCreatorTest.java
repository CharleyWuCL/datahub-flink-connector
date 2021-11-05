package charley.wu.flink.datahub.tool;

/**
 * Desc...
 *
 * @author Charley Wu
 * @since 2021/11/05
 */
public class TopicCreatorTest{

  public static void main(String[] args) {
    try {
      TopicCreator creator = new TopicCreator("阿里云AccessId", "阿里云AccessKey");
      creator.createTopic("test_project", TestMessage.class, true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
