package demo.jsqlparser;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;
import java.util.Stack;

public class JsqlparserDemo {

    @Test
    public void simpleSqlParsing() throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse("select * from table1;");
        Select selectStatement = (Select)statement;

        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        for (String tableName : tableList) {
            System.out.println(tableName);
        }

    }

    @Test
    public void sqlScriptParsing() throws JSQLParserException {
        Statements statements = CCJSqlParserUtil.parseStatements("select * from table1; select * from table2");
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<Statement> statementList = statements.getStatements();
        for(Statement statement : statementList) {
            List<String> tableList = tablesNamesFinder.getTableList(statement);
            for (String tableName : tableList) {
                System.out.println(tableName);
            }
        }
    }

    @Test
    public void tablePicker() {
        String sql = "insert into a select * from b";
        CCJSqlParserManager parser = new CCJSqlParserManager();

        try {
            Statement statement = parser.parse(new StringReader(sql));

            // 如果是查询处理
            if (statement instanceof Select) {
                System.out.println("select");
                Select select = (Select)statement;


            }

            if (statement instanceof Insert) {
                System.out.println("insert");
                Insert insert = (Insert)statement;

                // 获取insert语句中的查询语句
                Select select = insert.getSelect();
                if (select != null) {

                }


            }







        } catch (JSQLParserException e) {
            e.printStackTrace();
        }


    }


    @Test
    public void evaluate() throws JSQLParserException {
        evaluate("4+5*6");
        evaluate("4*5+6");
        evaluate("4*(5+6)");
        evaluate("4*(5+6)*(2+3)");

    }

    static void evaluate(String expr) throws JSQLParserException {
        final Stack<Long> stack = new Stack<Long>();
        System.out.println("expr=" + expr);
        Expression parseExpression = CCJSqlParserUtil.parseExpression(expr);
        ExpressionDeParser deparser = new ExpressionDeParser() {
            @Override
            public void visit(Addition addition) {
                super.visit(addition);

                long sum1 = stack.pop();
                long sum2 = stack.pop();

                stack.push(sum1 + sum2);
            }

            @Override
            public void visit(Multiplication multiplication) {
                super.visit(multiplication);

                long fac1 = stack.pop();
                long fac2 = stack.pop();

                stack.push(fac1 * fac2);
            }

            @Override
            public void visit(LongValue longValue) {
                super.visit(longValue);
                stack.push(longValue.getValue());
            }
        };
        StringBuilder b = new StringBuilder();
        deparser.setBuffer(b);
        parseExpression.accept(deparser);

        System.out.println(expr + " = " + stack.pop() );
    }



}
