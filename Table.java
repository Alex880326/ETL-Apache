import javax.persistence.*;
import java.util.Date;
import java.util.List;

@Entity
@Table(name = "original_table")
public class OriginalEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String data;

    private Date timestamp;

}

@Repository
public interface OriginalEntityRepository extends JpaRepository<OriginalEntity, Long> {
}

@Service
public class DataService {

    @Autowired
    private OriginalEntityRepository originalEntityRepository;

      @Transactional
    public void processAndSaveData() {
        List<OriginalEntity> originalData = originalEntityRepository.findAll();

        for (OriginalEntity originalEntity : originalData) {
            originalEntity.setTimestamp(new Date());
        }

        for (OriginalEntity originalEntity : originalData) {
            NewEntity newEntity = new NewEntity();
            newEntity.setData(originalEntity.getData());
            newEntity.setTimestamp(originalEntity.getTimestamp());
                     newEntityRepository.save(newEntity);
        }
    }
}

@Entity
@Table(name = "new_table")
public class NewEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String data;

    private Date timestamp;

}

@Repository
public interface NewEntityRepository extends JpaRepository<NewEntity, Long> {
}
