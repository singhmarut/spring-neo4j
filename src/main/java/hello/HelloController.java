package hello;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

import org.neo4j.driver.v1.*;

import javax.websocket.server.PathParam;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

@RestController
public class HelloController {

    @RequestMapping("/")
    public String index() {
        return "Greetings from Spring Boot!";
    }

    @Autowired
    Driver graphDatabase;

    @RequestMapping("/user/graph")
    public void popuplateGraph(@RequestParam("fileName") String fileName) {

        Reader in = null;
        try {
            in = new FileReader(String.format("/Users/marutsingh/Downloads/%s",fileName));

            //Iterable<CSVRecord> records = CSVFormat.EXCEL.parse(in);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);

            Session session = graphDatabase.session();
            Transaction transaction = session.beginTransaction();

            //transaction.run("CREATE CONSTRAINT ON (user:Login) ASSERT user.name IS UNIQUE");

            for (CSVRecord record : records) {
                String login = record.get("LOGIN").trim();
                String account = record.get("AGENT_ACCOUNT").trim();

                String loginNode = String.format("MERGE (n:Login {id: '%s'})", login);
                String accountNode = String.format("MERGE (n:Account {id: '%s'})", account);

                transaction.run(loginNode);
                //parameters("account",account)
                transaction.run(accountNode);

                try {
                    String relation = String.format(" MATCH (a:Account { id: '%s' }) \n" +
                            "MATCH (l:Login { id: '%s' }) MERGE (a)-[:HAS_LOGIN]-(l) ", account,login);
                    transaction.run(relation);
                }catch (Exception ex){
                    ex.printStackTrace();
                }
           /* session.run( "CREATE (login:Login {name: {name}, title: {title}})",
                    parameters( "name", "Arthur", "title", "King" ) );*/

            }

            transaction.success();
            transaction.close();
            session.close();
            //graphDatabase.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //return "Greetings from Spring Boot!";
    }

    /**
     * returns all logins for a particular account
     * @param accountId
     */
    @RequestMapping("/account/{accountId}/login/list")
    List<String> findAllLogins(@PathVariable("accountId") String accountId){

        Session session = graphDatabase.session();
        StatementResult result = session.run( "MATCH (a:Account {id: {accountId}})-[:HAS_LOGIN]->(login)\n" +
                "                RETURN a.id, login.id",parameters("accountId",accountId) );


        List<String> accountIds = new ArrayList<>();
        while ( result.hasNext() )
        {
            Record record = result.next();
            accountIds.add(record.get( "login.id" ).asString());
        }

        return accountIds;
    }
}
