package com.example.multitenantsql;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * todo demonstrate authentication that delegates to a sql db by partition and demonstrate
 */
@SpringBootApplication
public class MultitenantSqlApplication {

    public static void main(String[] args) {
        SpringApplication.run(MultitenantSqlApplication.class, args);
    }

    private final Map<Object, Object> dataSources = new ConcurrentHashMap<>();
    private final Map<String, MultitenantUser> users = new ConcurrentHashMap<>();


    @Bean
    BeanPostProcessor dataSourceBeanPostProcessor() {
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

    private void login(String username, String password) {
        var auth = new MultitenantAuthentication(username, password, this.users.get(username));
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @Bean
    ApplicationRunner runner(MultitenantRoutingDataSource mds, CustomerService customerService) {
        return args -> {
            mds.afterPropertiesSet();

            this.login("jlong", "pw");
            Stream.of("Rob", "Yuxin", "Tasha", "Mark", "Olga", "Violetta", "Illaya", "Sabby")
                    .map(customerService::create)
                    .forEach(System.out::println);
            customerService.findAll().forEach(System.out::println);

            this.login("rwinch", "pw");
            Stream.of("Rod", "Dave", "Jürgen")
                    .map(customerService::create)
                    .forEach(System.out::println);
            customerService.findAll().forEach(System.out::println);
        };
    }


    @Bean
    DataSource ds1() {
        return this.initializeDataSource();
    }

    @Bean
    DataSource ds2() {
        return this.initializeDataSource();
    }

    @Bean
    UserDetailsService userDetailsService() {
        var rob = this.user("rwinch", "pw", 1, "USER", "ADMIN");
        var josh = this.user("jlong", "pw", 2, "USER");
        this.users.putAll(Stream.of(josh, rob).collect(Collectors.toMap(User::getUsername, user -> (MultitenantUser) user)));
        return username -> {
            var r = this.users.getOrDefault(username, null);
            if (r == null) throw new UsernameNotFoundException("couldn't find the user '" + username + "'");
            return r;
        };
    }

    private DataSource initializeDataSource() {
        return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).addScript("schema.sql").build();
    }

    private User user(String user, String pw, Integer tenantId, String... roles) {
        var auths = Arrays.stream(roles).map(SimpleGrantedAuthority::new).toList();
        return new MultitenantUser(user, pw, true, true, true, true, auths, tenantId);
    }

    @Bean
    @Primary
    MultitenantRoutingDataSource routingDataSource() {
        var mds = new MultitenantRoutingDataSource();
        mds.setTargetDataSources(this.dataSources);
        return mds;
    }

}

class MultitenantRoutingDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        var auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof MultitenantAuthentication user)
            return user.getUser().getTenantId();
        return null;
    }
}

class MultitenantAuthentication extends PreAuthenticatedAuthenticationToken {

    private final MultitenantUser user;

    public MultitenantAuthentication(Object aPrincipal, Object aCredentials, MultitenantUser user) {
        super(aPrincipal, aCredentials);
        this.user = user;
    }

    public MultitenantUser getUser() {
        return user;
    }
}

class MultitenantUser extends User {

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


@Service
class CustomerService {

    private final JdbcTemplate template;

    CustomerService(JdbcTemplate template) {
        this.template = template;
    }

    Customer create(String name) {
        var kh = new GeneratedKeyHolder();
        var update = this.template.update(con -> {
            var ps = con.prepareStatement("insert into customer (name ) values(? )");
            ps.setString(1, name);
            return ps;
        }, kh);
        Assert.isTrue(update > 0, () -> "there should be more than one updated row!");
        return new Customer(kh.getKey().intValue(), name);
    }

    Collection<Customer> findAll() {
        return this.template.query("select * from customer  ",
                (rs, rowNum) -> new Customer(rs.getInt("id"), rs.getString("name")));
    }


}

record Customer(Integer id, String name) {
}
