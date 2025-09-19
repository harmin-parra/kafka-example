package cassandra;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class UserProfile {

    private UUID id;
    private String name;
    private Integer age;
    private Set<String> tags;
    private Instant createdAt;

    // Constructors
    public UserProfile() {}

    public UserProfile(UUID id, String name, Integer age, Set<String> tags, Instant createdAt) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.tags = tags;
        this.createdAt = createdAt;
    }

    // Getters and setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }

    public Set<String> getTags() { return tags; }
    public void setTags(Set<String> tags) { this.tags = tags; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
