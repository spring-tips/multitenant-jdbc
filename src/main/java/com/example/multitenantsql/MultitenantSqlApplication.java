package com.example.multitenantsql;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
    RouterFunction<ServerResponse> routes(JdbcTemplate template) {
        return route()
                .GET("/customers", request -> {
                    var results = template
                            .query("select * from customer", (rs, i) -> new Customer(rs.getInt("id"), rs.getString("name")));
                    return ServerResponse.ok().body(results);
                })
                .build();
    }
}

record Customer(Integer id, String name) {
}

@Configuration
class SecurityConfiguration {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .httpBasic(withDefaults())
                .authorizeHttpRequests(auth -> auth.anyRequest().authenticated())
                .csrf(AbstractHttpConfigurer::disable);
        return http.build();
    }

    @Bean
    UserDetailsService userDetailsService() {
        var rob = this.createUser("rwinch", "pw", 1);
        var josh = this.createUser("jlong", "pw", 2);
        var users = Stream.of(josh, rob)
                .collect(Collectors.toMap(User::getUsername, user -> user));
        return username -> {
            var user = users.getOrDefault(username, null);
            if (user == null) throw new UsernameNotFoundException("couldn't find the user '" + username + "'");
            return user;
        };
    }

    private User createUser(String user, String pw, Integer tenantId) {
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
            super(username, PasswordEncoderFactories.createDelegatingPasswordEncoder().encode(password), enabled, accountNonExpired, credentialsNonExpired, accountNonLocked, authorities);
            this.tenantId = tenantId;
        }
    }

}

@Configuration
class DataSourceConfiguration {


    @Bean
    @Primary
    MultitenantDataSource multitenantDataSource(Map<String, DataSource> dataSources) {
        var prefix = "ds";
        var map = dataSources
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(e -> {
                    var k = e.getKey();
                    var part = k.substring(prefix.length());
                    var num = Integer.parseInt(part);
                    return (Object) num;
                }, e -> (Object) e.getValue()));
        map.forEach((tenantId, ds) -> {
            var initializer = new ResourceDatabasePopulator(
                    new ClassPathResource("schema.sql"),
                    new ClassPathResource(prefix + tenantId + "-data.sql"));
            initializer.execute((DataSource) ds);
            System.out.println("initialized " + tenantId);
        });
        System.out.println("there are " + map.size() + " dataSources");
        var mds = new MultitenantDataSource();
        mds.setTargetDataSources(map);
        return mds;
    }

    @Bean
    DataSource ds1() {
        return createNewDataSource(5431);
    }

    @Bean
    DataSource ds2() {
        return createNewDataSource(5432);
    }

    private static DataSource createNewDataSource(int port) {
        var dsp = new DataSourceProperties();
        dsp.setUsername("user");
        dsp.setPassword("pw");
        dsp.setUrl("jdbc:postgresql://localhost:" + port + "/user");
        return dsp
                .initializeDataSourceBuilder()//
                .type(HikariDataSource.class)//
                .build();
    }

    private static class MultitenantDataSource extends AbstractRoutingDataSource {

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
            if (auth != null && auth.getPrincipal() instanceof SecurityConfiguration.MultitenantUser user) {
                var tenantId = user.getTenantId();
                System.out.println("  " + user.getUsername() + " / " + tenantId);
                return tenantId;
            }
            return null;
        }
    }

}