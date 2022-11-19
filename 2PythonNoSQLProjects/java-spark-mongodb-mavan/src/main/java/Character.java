import java.io.Serializable;

public final class Character implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String name;
    private Integer age;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public Integer getAge() {
        return age;
    }
    public void setAge(final Integer age) {
        this.age = age;
    }
}