package org.apache.spark.sql.rasql.logical

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Ascending, AttributeReference, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, Descending, Divide, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Like, Literal, Multiply, Not, Or, RLike, Remainder, SortDirection, SortOrder, Subtract, UnaryMinus, aggregate}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.catalyst.{AbstractSparkSQLParser, InternalRow, TableIdentifier}
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SQLContext}

object RaSQLParser extends AbstractSparkSQLParser with DataTypeParser {

    // TODO add partitioning

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

    @transient
    private final val rasqlContext: RaSQLContext = SQLContext.getActive().get.asInstanceOf[RaSQLContext]


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

    // TODO Comment
    protected override lazy val start: Parser[LogicalPlan] = rasql
    var rrAttributes: Seq[AttributeReference] = _
    var fName: String = _
    var attrAlias: String = _

    lazy val rasql: Parser[LogicalPlan] =
        (recursive_cte) ~
            (AS ~> ( "(" ~> simple_select <~ ")")) ~
            (UNION ~> ("(" ~> recursive_select <~ ")")) ~
            (simple_select) ^^{
            case t ~ ss ~ rs ~ s =>

                rasqlContext.recursiveTable = t
                val rsAttr = rs.projectList.last
                val rsAf = UnresolvedFunction(fName, Seq(rsAttr), isDistinct = true)
                val rsAggregate = UnresolvedAlias(Alias(rsAf, attrAlias)())
                val rsGroupByArgs = rs.projectList.dropRight(1)
                val rsAggregateArgs = rsGroupByArgs :+ rsAggregate
                val rsMA = MonotonicAggregate(rsGroupByArgs, rsAggregateArgs, rs)

                val ssAttr = ss.projectList.last
                val ssAf = UnresolvedFunction(fName, Seq(ssAttr), isDistinct = true)
                val ssAggregate = UnresolvedAlias(Alias(ssAf, attrAlias)())
                val ssGroupByArgs = ss.projectList.dropRight(1)
                val ssAggregateArgs = ssGroupByArgs :+ ssAggregate
                val ssMA = MonotonicAggregate(ssGroupByArgs, ssAggregateArgs, ss)

                val ra = AggregateRecursion(t, ssMA, rsMA)
                //val pr = Project(rrAttributes ++ rs.projectList, ra)
                val w = With(s, Map(t-> Subquery(t, ra)))
                w
    }

    lazy val recursive_cte : Parser[String] =
        (WITH ~> (RECURSIVE ~> ident)) ~ ("(" ~> projection <~ ",") ~ (premFunction <~ ")") ^^{
            case t ~ p ~ pf =>
                //val t_ = t+"_"
                fName = pf.getFunctionName
                attrAlias = pf.getTargetAttributeAlias
                val urp = p.asInstanceOf[UnresolvedAttribute].name

                // todo schema is hardcoded
                val structFields = Seq(
                    StructField(urp, LongType, nullable = false),
                    StructField(attrAlias, LongType, nullable = false)
                )
                val schema = StructType(structFields)
                rrAttributes = structFields.map(sf => AttributeReference(sf.name, sf.dataType, nullable = false)())

                val rrdd = rasqlContext.sparkContext.emptyRDD[InternalRow]
                val rdf = rasqlContext.internalCreateDataFrame(t, rrdd, schema).toDF(rrAttributes.map(_.name):_*)
                rasqlContext.setRecursiveRDD(t, rrdd)
                rasqlContext.registerDataFrameAsTable(rdf, t)
                t
        }


    lazy val recursive_select: Parser[Project] =
        SELECT ~> repsep(projection, ",") ~
            (FROM   ~> recursive_relations) ~
            (WHERE  ~> expression).? ^^ {
            case p ~ rr ~ c =>
                val rrJoin = rr.asInstanceOf[Join]
                val joined  = Join(rrJoin.left, rrJoin.right, Inner, c)
                val withProject = Project(p.map(UnresolvedAlias), joined)
                withProject
        }

    protected lazy val simple_select: Parser[Project] =
        SELECT ~> rep1sep(projection, ",") ~
            (FROM   ~> relations).? ~
            (WHERE  ~> expression).? ^^ {
            case p ~ r ~ f =>
                val base = r.getOrElse(OneRowRelation)
                val withFilter = f.map(Filter(_, base)).getOrElse(base)
                val withProject = Project(p.map(UnresolvedAlias), withFilter)
                withProject
        }

    lazy val premFunction: Parser[RecursiveAggregateExpr] =
        ident ~ (AS ~> expression) ^^ { case premF ~ e =>
            lexical.normalizeKeyword(premF) match {
                case "mcount" => RecursiveAggregateExpr(MCount(e), mode = Complete, fName = "mcount", attrName = e)
                case "msum" => RecursiveAggregateExpr(MSum(e), mode = Complete, fName = "msum", attrName = e)
                case "mmin" => RecursiveAggregateExpr(MMin(e), mode = Complete, fName = "mmin", attrName = e)
                case "mmax" => RecursiveAggregateExpr(MMax(e), mode = Complete, fName = "mmax", attrName = e)
                case _ => throw new AnalysisException(s"invalid expression $premF")
            }
        }

    lazy val projection: Parser[Expression] = expression

    lazy val recursive_relations: Parser[LogicalPlan] =
        (tableIdentifier ~ rep1("," ~> relation) ^^ {case rr ~ joins => joins.foldLeft(Subquery(rr.unquotedString, RecursiveRelation(rr, rrAttributes)).asInstanceOf[LogicalPlan]) { case(lhs, r) => Join(lhs, r, Inner, None) } }
            | tableIdentifier ^^ {case rr => Subquery(rr.unquotedString, RecursiveRelation(rr, rrAttributes))})

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
            | function
            | dotExpressionHeader
            | signedPrimary
            | "~" ~> expression ^^ BitwiseNot
            | attributeName ^^ UnresolvedAttribute.quoted
            )

    protected lazy val function: Parser[Expression] =
        ( ident <~ ("(" ~ "*" ~ ")") ^^ { case udfName =>
            if (lexical.normalizeKeyword(udfName) == "count") {
                AggregateExpression(Count(Literal(1)), mode = Complete, isDistinct = false)
            } else {
                throw new AnalysisException(s"invalid expression $udfName(*)")
            }
        }
            | ident ~ ("(" ~> repsep(expression, ",")) <~ ")" ^^
            { case udfName ~ exprs => UnresolvedFunction(udfName, exprs, isDistinct = false) }
            | ident ~ ("(" ~ DISTINCT ~> repsep(expression, ",")) <~ ")" ^^ { case udfName ~ exprs =>
            lexical.normalizeKeyword(udfName) match {
                case "count" =>
                    aggregate.Count(exprs).toAggregateExpression(isDistinct = true)
                case _ => UnresolvedFunction(udfName, exprs, isDistinct = true)
            }
        }
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
