package org.apache.spark.sql.rasql

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{AbstractSparkSQLParser, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.expressions.{Add, And, Ascending, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, Descending, Divide, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Like, Literal, Multiply, Not, Or, RLike, Remainder, SortDirection, SortOrder, Subtract, UnaryExpression, UnaryMinus, aggregate}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Filter, Join, LogicalPlan, OneRowRelation, Project, Sort, Union}
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, CreateTableUsingAsSelect}
import org.apache.spark.sql.types.{BooleanType, NullType, StringType, StructType}


object RaSQLParser extends AbstractSparkSQLParser with DataTypeParser {

    val DISTINCT: Keyword = Keyword("DISTINCT")
    val SELECT: Keyword = Keyword("SELECT")
    val FROM: Keyword = Keyword("FROM")
    val WHERE: Keyword = Keyword("WHERE")
    val AS: Keyword = Keyword("AS")
    val UNION: Keyword = Keyword("UNION")
    val ORDER: Keyword = Keyword("ORDER")
    val SORT: Keyword = Keyword("SORT")
    val BY: Keyword = Keyword("BY")
    val ASC: Keyword = Keyword("ASC")
    val DESC: Keyword = Keyword("DESC")
    val LIMIT: Keyword = Keyword("LIMIT")

    val OR: Keyword = Keyword("OR")
    val AND: Keyword = Keyword("AND")
    val NOT: Keyword = Keyword("NOT")
    val LIKE: Keyword = Keyword("LIKE")
    val BETWEEN: Keyword = Keyword("BETWEEN")
    val IN: Keyword = Keyword("IN")
    val IS: Keyword = Keyword("IS")
    val NULL: Keyword = Keyword("NULL")
    val TRUE: Keyword = Keyword("TRUE")
    val FALSE: Keyword = Keyword("FALSE")
    val CAST: Keyword = Keyword("CAST")

    protected val WITH: Keyword = Keyword("with")
    protected val RECURSIVE: Keyword = Keyword("recursive")
    protected val REGEXP: Keyword = Keyword("REGEXP")

    /*Example:
    | Base tables: edge(Src: int, Dst: int) // omitted
    |
    | WITH recursive cc(Src, min() AS CmpId) AS
    | (SELECT Src, Src FROM edge) UNION
    | (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src)
    |  SELECT count(distinct cc.CmpId) FROM cc

    | WITH recursive cc(Src, min() AS CmpId) AS
    | (SELECT Src, Src FROM edge) UNION
    | (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src)
    |  SELECT count(distinct cc.CmpId) FROM cc
 */

// WITH recursive cc(Src, min() AS CmpId) AS (SELECT Src, Src FROM edge) UNION  (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src)
    protected override lazy val start: Parser[LogicalPlan] = rasql

    lazy val rasql: Parser[LogicalPlan] =
        (WITH ~> (RECURSIVE ~> tableIdentifier)) ~ ("(" ~> projection <~ ",") ~ (premFunction <~ ")") ~
        (AS ~> ( "(" ~> simple_select <~ ")")) ~
        (UNION ~> ("(" ~> recursive_select <~ ")")) ^^{
            case t ~ p ~ pf ~ ss ~ rs =>
                val fName = pf.getFunctionName
                val attr = Seq(pf.getTargetAttribute)
                val af = UnresolvedFunction(fName, attr, isDistinct = false)

                val agss = Project(Seq(UnresolvedAlias(af)), ss)
                val agrs = Project(Seq(UnresolvedAlias(af)), rs)
                val u = Union(agss, agrs)
                val rt = CreateTableUsingAsSelect(t, "", temporary = true, Array.empty[String], SaveMode.Ignore, Map(), u)
                rt
    }


    lazy val recursive_select: Parser[LogicalPlan] =
        SELECT ~> DISTINCT.? ~
            repsep(projection, ",") ~
            (FROM   ~> recursive_relations).? ~
            (WHERE  ~> expression).?  ^^ {
            case d ~ p ~ r ~ f =>
                val base = r.getOrElse(OneRowRelation)
                val withFilter = f.map(Filter(_, base)).getOrElse(base)
                val projection = Project(p.map(UnresolvedAlias), withFilter)
                val withDistinct = d.map(_ => Distinct(projection)).getOrElse(projection)
                withDistinct
        }

    lazy val simple_select: Parser[LogicalPlan] =
        SELECT ~ repsep(projection, ",") ~ (FROM   ~> relations).? ^^ {
            case p ~ r  => Project(p._2.map(UnresolvedAlias), r.getOrElse(OneRowRelation))
        }

    lazy val premFunction: Parser[RecursiveAggregateExpr] =
        ident ~ (AS ~> expression) ^^ { case premF ~ e =>
            lexical.normalizeKeyword(premF) match {
                case "count" => RecursiveAggregateExpr(Count(e), mode = Complete, fName = "count", attrName = e)
                case "sum" => RecursiveAggregateExpr(Sum(e), mode = Complete, fName = "sum", attrName = e)
                case "min" => RecursiveAggregateExpr(Min(e), mode = Complete, fName = "min", attrName = e)
                case "max" => RecursiveAggregateExpr(Max(e), mode = Complete, fName = "max", attrName = e)
                case _ => throw new AnalysisException(s"invalid expression $premF")
            }
        }

    lazy val projection: Parser[Expression] = expression

    lazy val recursive_relations: Parser[LogicalPlan] =
        relation ~ ("," ~> relation) ^^ {case r1 ~ r2 => Join(r1, r2, Inner, None) }

    lazy val relations: Parser[LogicalPlan] =
        (relation ~ rep1("," ~> relation) ^^ {case r1 ~ joins => joins.foldLeft(r1) { case(lhs, r) => Join(lhs, r, Inner, None) } }
            | relation)

    lazy val relation: Parser[LogicalPlan] =
        tableIdentifier ~ (opt(AS) ~> opt(ident)) ^^ {case tableIdent ~ alias => UnresolvedRelation(tableIdent, alias)}

    lazy val tableIdentifier: Parser[TableIdentifier] =
        (ident <~ ".").? ~ ident ^^ { case maybeDbName ~ tableName => TableIdentifier(tableName, maybeDbName) }

    lazy val sortType: Parser[LogicalPlan => LogicalPlan] =
        (ORDER ~ BY  ~> ordering ^^ { case o => l: LogicalPlan => Sort(o, true, l) } | SORT ~ BY  ~> ordering ^^ { case o => l: LogicalPlan => Sort(o, false, l) } )

    lazy val ordering: Parser[Seq[SortOrder]] = rep1sep(expression ~ direction.? , ",") ^^ {case exps => exps.map(pair => SortOrder(pair._1, pair._2.getOrElse(Ascending)))}

    lazy val direction: Parser[SortDirection] = ASC  ^^^ Ascending | DESC ^^^ Descending

    lazy val expression: Parser[Expression] = orExpression

    lazy val orExpression: Parser[Expression] = andExpression * (OR ^^^ { (e1: Expression, e2: Expression) => Or(e1, e2) })

    lazy val andExpression: Parser[Expression] = notExpression * (AND ^^^ { (e1: Expression, e2: Expression) => And(e1, e2) })

    lazy val notExpression: Parser[Expression] = NOT.? ~ comparisonExpression ^^ { case maybeNot ~ e => maybeNot.map(_ => Not(e)).getOrElse(e) }

    lazy val comparisonExpression: Parser[Expression] =
        ( termExpression ~ ("="  ~> termExpression) ^^ { case e1 ~ e2 => EqualTo(e1, e2) }
            | termExpression ~ ("<"  ~> termExpression) ^^ { case e1 ~ e2 => LessThan(e1, e2) }
            | termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => LessThanOrEqual(e1, e2) }
            | termExpression ~ (">"  ~> termExpression) ^^ { case e1 ~ e2 => GreaterThan(e1, e2) }
            | termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => GreaterThanOrEqual(e1, e2) }
            | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
            | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
            | termExpression ~ ("<=>" ~> termExpression) ^^ { case e1 ~ e2 => EqualNullSafe(e1, e2) }
            | termExpression ~ NOT.? ~ (BETWEEN ~> termExpression) ~ (AND ~> termExpression) ^^ {
            case e ~ not ~ el ~ eu =>
                val betweenExpr: Expression = And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))
                not.fold(betweenExpr)(f => Not(betweenExpr))
        }
            | termExpression ~ (REGEXP ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) }
            | termExpression ~ (LIKE   ~> termExpression) ^^ { case e1 ~ e2 => Like(e1, e2) }
            | termExpression ~ (NOT ~ LIKE ~> termExpression) ^^ { case e1 ~ e2 => Not(Like(e1, e2)) }
            | termExpression ~ (IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
            case e1 ~ e2 => In(e1, e2)
        }
            | termExpression ~ (NOT ~ IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
            case e1 ~ e2 => Not(In(e1, e2))
        }
            | termExpression <~ IS ~ NULL ^^ { case e => IsNull(e) }
            | termExpression <~ IS ~ NOT ~ NULL ^^ { case e => IsNotNull(e) }
            | termExpression
            )

    lazy val termExpression: Parser[Expression] =
        productExpression *
            ( "+" ^^^ { (e1: Expression, e2: Expression) => Add(e1, e2) }
                | "-" ^^^ { (e1: Expression, e2: Expression) => Subtract(e1, e2) }
                )

    lazy val productExpression: Parser[Expression] =
        baseExpression *
            ( "*" ^^^ { (e1: Expression, e2: Expression) => Multiply(e1, e2) }
                | "/" ^^^ { (e1: Expression, e2: Expression) => Divide(e1, e2) }
                | "%" ^^^ { (e1: Expression, e2: Expression) => Remainder(e1, e2) }
                | "&" ^^^ { (e1: Expression, e2: Expression) => BitwiseAnd(e1, e2) }
                | "|" ^^^ { (e1: Expression, e2: Expression) => BitwiseOr(e1, e2) }
                | "^" ^^^ { (e1: Expression, e2: Expression) => BitwiseXor(e1, e2) }
                )

    lazy val baseExpression: Parser[Expression] =
        ( "*" ^^^ UnresolvedStar(None)
            | rep1(ident <~ ".") <~ "*" ^^ { case target => UnresolvedStar(Option(target))}
            | primary
            )

    lazy val primary: PackratParser[Expression] =
        ( literal
            | expression ~ ("[" ~> expression <~ "]") ^^{ case base ~ ordinal => UnresolvedExtractValue(base, ordinal) }
            | (expression <~ ".") ~ ident ^^
            { case base ~ fieldName => UnresolvedExtractValue(base, Literal(fieldName)) }
            | "(" ~> expression <~ ")"
            | dotExpressionHeader
            | signedPrimary
            | "~" ~> expression ^^ BitwiseNot
            | attributeName ^^ UnresolvedAttribute.quoted
            )


    lazy val dotExpressionHeader: Parser[Expression] = (ident <~ ".") ~ ident ~ rep("." ~> ident) ^^ { case i1 ~ i2 ~ rest => UnresolvedAttribute(Seq(i1, i2) ++ rest) }

    lazy val sign: Parser[String] = ("+" | "-")

    lazy val signedPrimary: Parser[Expression] = sign ~ primary ^^ { case s ~ e => if (s == "-") UnaryMinus(e) else e }

    lazy val literal: Parser[Literal] =
        ( numericLiteral
            | booleanLiteral
            | stringLit ^^ { case s => Literal.create(s, StringType) }
            | NULL ^^^ Literal.create(null, NullType)
            )
    lazy val booleanLiteral: Parser[Literal] =
        ( TRUE ^^^ Literal.create(true, BooleanType)
            | FALSE ^^^ Literal.create(false, BooleanType)
            )
    lazy val attributeName: Parser[String] = acceptMatch("attribute name", {
        case lexical.Identifier(str) => str
        case lexical.Keyword(str) if !lexical.delimiters.contains(str) => str
    })

    lazy val numericLiteral: Parser[Literal] =
        ( integral  ^^ { case i => Literal(toNarrowestIntegerType(i))}
            | sign.? ~ unsignedFloat ^^
            { case s ~ f => Literal(toDecimalOrDouble(s.getOrElse("") + f)) }
            )

    private def toNarrowestIntegerType(value: String): Any = {
        val bigIntValue = BigDecimal(value)

        bigIntValue match {
            case v if bigIntValue.isValidInt => v.toIntExact
            case v if bigIntValue.isValidLong => v.toLongExact
            case v => v.underlying()
        }
    }

    lazy val unsignedFloat: Parser[String] = ( "." ~> numericLit ^^ { u => "0." + u } | elem("decimal", _.isInstanceOf[lexical.DecimalLit]) ^^ (_.chars))

    lazy val integral: Parser[String] = sign.? ~ numericLit ^^ { case s ~ n => s.getOrElse("") + n }

    private def toDecimalOrDouble(value: String): Any = {
        val decimal = BigDecimal(value)
        // follow the behavior in MS SQL Server
        // https://msdn.microsoft.com/en-us/library/ms179899.aspx
        if (value.contains('E') || value.contains('e')) {
            decimal.doubleValue()
        } else {
            decimal.underlying()
        }
    }

}

