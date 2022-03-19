package com.example.multitenantsql;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;

import javax.sql.DataSource;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.security.config.Customizer.withDefaults;
import static org.springframework.web.servlet.function.RouterFunctions.route;

/**
 * todo demonstrate authentication that delegates to a sql db by partition
 * before running this, you'll need to spin up some Postgres instances on two different ports. I'm using `5432` and `5431`.
 */
@SpringBootApplication
public class MultitenantSqlApplication {

    public static void main(String[] args) {
        SpringApplication.run(MultitenantSqlApplication.class, args);
    }

    @Bean
    RouterFunction<ServerResponse> http(CustomerService customerService) {
        return route()
                .GET("/customers", request -> ServerResponse.ok().body(customerService.findAll()))
                .build();
    }
}

@Configuration
class SecurityConfiguration {

    private final Map<String, MultitenantUser> users = new ConcurrentHashMap<>();

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .authorizeHttpRequests(authz -> authz.anyRequest().authenticated())
                .httpBasic(withDefaults())
                .build();
    }

    @Bean
    UserDetailsService userDetailsService() {
        var rob = this.user("rwinch", "{noop}pw", 1);
        var josh = this.user("jlong", "{noop}pw", 2);
        this.users.putAll(Stream.of(josh, rob).collect(Collectors.toMap(User::getUsername, user -> (MultitenantUser) user)));
        return username -> {
            var user = this.users.getOrDefault(username, null);
            if (user == null) throw new UsernameNotFoundException("couldn't find the user '" + username + "'");
            return user;
        };
    }

    private User user(String user, String pw, Integer tenantId) {
        return new MultitenantUser(user, pw, true, true, true, true, List.of(new SimpleGrantedAuthority("USER")), tenantId);
    }

    static class MultitenantUser extends User {

        private final Integer tenantId;

        public Integer getTenantId() {
            return tenantId;
        }

        public MultitenantUser(String username, String password, boolean enabled, boolean accountNonExpired,
                               boolean credentialsNonExpired, boolean accountNonLocked,
                               Collection<? extends GrantedAuthority> authorities, Integer tenantId) {
            super(username, password, enabled, accountNonExpired, credentialsNonExpired, accountNonLocked, authorities);
            this.tenantId = tenantId;
        }
    }
}


@Configuration
class DataSourceConfiguration {

    private final Map<Object, Object> dataSources = new ConcurrentHashMap<>();

    @Bean
    BeanPostProcessor multitenantDataSourceBeanPostProcessor() {
        return new BeanPostProcessor() {

            @Override
            public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
                var prefix = "ds";
                if (beanName.startsWith(prefix)) {
                    var tenantId = Integer.parseInt(beanName.substring(prefix.length()));
                    dataSources.put(tenantId, bean);
                    System.out.println("assigning " + tenantId + " to " + bean);
                }

                return BeanPostProcessor.super.postProcessBeforeInitialization(bean, beanName);
            }


        };
    }

    @Bean
    @Primary
    MultitenantRoutingDataSource multitenantDataSource() {
        var mds = new MultitenantRoutingDataSource();
        mds.setTargetDataSources(this.dataSources);
        return mds;
    }

    @Bean
    DataSource ds1() {
        return this.createNewDataSource(5431);
    }

    @Bean
    DataSource ds2() {
        return this.createNewDataSource(5432);
    }

    private DataSource createNewDataSource(int port) {
        var dsp = new DataSourceProperties();
        dsp.setUsername("user");
        dsp.setPassword("pw");
        dsp.setUrl("jdbc:postgresql://localhost:" + port + "/user");
        var dataSource = dsp.initializeDataSourceBuilder().type(HikariDataSource.class).build();
        if (StringUtils.hasText(dsp.getName())) {
            dataSource.setPoolName(dsp.getName());
        }
        var initializer = new ResourceDatabasePopulator(new ClassPathResource("schema.sql"));
        initializer.execute(dataSource);
        return dataSource;
    }

    @Bean
    ApplicationRunner sampleDataInitializer(Map<String, DataSource> dataSourceMap) {
        return args -> {
            var data = Map.of(
                    "ds1", List.of("Rod", "Dave", "Jürgen"),
                    "ds2", List.of("Rob", "Yuxin", "Tasha", "Mark", "Olga", "Violetta", "Illaya", "Sabby")
            );
            data.forEach((dbBeanName, customers) -> {
                var db = dataSourceMap.get(dbBeanName);
                var cs = new CustomerService(new JdbcTemplate(db));
                customers.forEach(cs::create);
            });
        };
    }


    static private class MultitenantRoutingDataSource extends AbstractRoutingDataSource {

        private final AtomicBoolean initialized = new AtomicBoolean();

        @Override
        protected DataSource determineTargetDataSource() {
            if (this.initialized.compareAndSet(false, true))
                this.afterPropertiesSet();
            return super.determineTargetDataSource();
        }

        @Override
        protected Object determineCurrentLookupKey() {
            var auth = SecurityContextHolder.getContext().getAuthentication();
            if (auth.getPrincipal() instanceof SecurityConfiguration.MultitenantUser user) {
                var tenantId = user.getTenantId();
                System.out.println("  " + user.getUsername() + " / " + tenantId);
                return tenantId;
            }
            return null;
        }
    }

}


@Service
class CustomerService {

    private final RowMapper<Customer> customerRowMapper =
            (rs, rowNum) -> new Customer(rs.getInt("id"), rs.getString("name"));
    private final JdbcTemplate template;

    CustomerService(JdbcTemplate template) {
        this.template = template;
    }

    Customer create(String name) {
        var sql = "INSERT INTO customer(name ) VALUES(?)";
        var decParams = List.of(new SqlParameter(Types.VARCHAR, "name"));
        var pscf = new PreparedStatementCreatorFactory(sql, decParams) {
            {
                setReturnGeneratedKeys(true);
                setGeneratedKeysColumnNames("id");
            }
        };
        var psc = pscf.newPreparedStatementCreator(List.of(name));
        var keyHolder = new GeneratedKeyHolder();
        template.update(psc, keyHolder);
        var id = Objects.requireNonNull(keyHolder.getKey()).intValue();
        return findById(id);

    }

    Customer findById(Integer id) {
        return this.template.queryForObject(
                "select * from customer where id = ? ", this.customerRowMapper, id);

    }

    Collection<Customer> findAll() {
        return this.template.query("select * from customer", this.customerRowMapper);
    }

}

record Customer(Integer id, String name) {
}
