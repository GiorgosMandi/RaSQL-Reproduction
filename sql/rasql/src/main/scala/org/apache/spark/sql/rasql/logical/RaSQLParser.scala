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

/**
 *  RaSQL Parser
 *
 *  Parses recursive queries into Spark Logical plans
 *
 *  The input queries must look like
 *
 *        WITH recursive recursive_table_name ( fields, (mmin|msum|mcount) AS name) AS
 *        | (BASE SELECT QUERY)
 *        | UNION
 *        | (RECURSIVE SELECT QUERIES)
 *        | (SELECT QUERY)
 */

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

    val MMIN: Keyword = Keyword("mmin")
    val MMAX: Keyword = Keyword("mmax")
    val MSUM: Keyword = Keyword("msum")
    val MCOUNT: Keyword = Keyword("mcount")

    protected val WITH: Keyword = Keyword("with")
    protected val RECURSIVE: Keyword = Keyword("recursive")
    protected val REGEXP: Keyword = Keyword("REGEXP")

    @transient
    private final val rasqlContext: RaSQLContext = SQLContext.getActive().get.asInstanceOf[RaSQLContext]

    protected override lazy val start: Parser[LogicalPlan] = rasql
    var attributes: Seq[AttributeReference] = _
    var aggregateFunction: AggregateFunction = _
    var partialPremF: AggregateExpression = _
    var finalPremF: AggregateExpression = _
    var premKey: UnresolvedAttribute = _
    var functionName: String =_
    var aggregationKey: UnresolvedAttribute = _
    var restAttributes: Seq[UnresolvedAttribute] = _

    lazy val rasql: Parser[LogicalPlan] =
        recursive_cte ~
            (AS ~> ( "(" ~> simple_select <~ ")")) ~
            (UNION ~> ("(" ~> recursive_select <~ ")")) ~
            simple_select ^^{
            case t ~ base ~ recursive ~ s =>

                rasqlContext.recursiveTable = t
                if(partialPremF != null) {

                    val aggrKeyUa = Seq(UnresolvedAttribute(aggregationKey.name))
                    val recPartialArgr = UnresolvedAlias(Alias(partialPremF, premKey.name)())
                    val recPartialAggregate = Aggregate(aggrKeyUa, aggrKeyUa :+ recPartialArgr, Subquery("RR", recursive))

                    val basePremKey = base.projectList.last
                    val basePartialAgr = AggregateExpression(getFunction(basePremKey, functionName), mode = Partial, isDistinct = true)
                    val basePartialAgrAliased = UnresolvedAlias(Alias(basePartialAgr, premKey.name)())
                    val basePartialAggregate = Aggregate(aggrKeyUa, aggrKeyUa :+ basePartialAgrAliased, base)

                    // In AggregateRecursion we apply the recursion
                    val ra = RecursiveAggregate(t, basePartialAggregate, recPartialAggregate)


                    // After Recursive Aggregation we apply again the PreM F
                    val finalAggregation = UnresolvedAlias(Alias(finalPremF, premKey.name)())
                    val raMA = Aggregate(aggrKeyUa, aggrKeyUa :+ finalAggregation, Subquery(t + "_RA", ra))

                    // After initializing the Recursive CTE we apply the select query
                    val w = With(s, Map(t -> Subquery(t, raMA)))
                    w
                }
                else {
                    val recAggrKey = UnresolvedAttribute(aggregationKey.name)
                    val recAggregateArgs = Seq(UnresolvedAlias(Alias(recAggrKey, aggregationKey.name)()))
                    val recAggregate = Aggregate(Seq(recAggrKey), recAggregateArgs, Subquery("RR", recursive))
                    val recursiveMAPartitioned =  RepartitionByExpression(Seq(recAggrKey), recAggregate, Option(rasqlContext.partitions))

                    val basePartitionKey = UnresolvedAttribute(attributes.head.name)
                    val baseMAPartitioned =  RepartitionByExpression(Seq(basePartitionKey), base, Option(rasqlContext.partitions))

                    val ra = RecursiveAggregate(t, baseMAPartitioned, recursiveMAPartitioned)
                    val w = With(s, Map(t -> Subquery(t, ra)))
                    w
                }
    }

    /**
     * Declaring the Recursive CTE
     */
    lazy val recursive_cte : Parser[String] =
        (WITH ~> (RECURSIVE ~> ident)) ~ ("(" ~> rep1sep( premFunction|projection, ",") <~ ")")  ^^{
            case t ~ p =>
                aggregationKey = p.head.asInstanceOf[UnresolvedAttribute]
                restAttributes = p.drop(1).asInstanceOf[Seq[UnresolvedAttribute]]
                val structFields =
                    if (premKey != null)
                        p.dropRight(1)
                            .map(ua => StructField(ua.asInstanceOf[UnresolvedAttribute].name, IntegerType, nullable = false))  :+
                            StructField(premKey.name, IntegerType, nullable = false)
                    else p.map(ua => StructField(ua.asInstanceOf[UnresolvedAttribute].name, IntegerType, nullable = false))

                // An empty RDD is initialized as the Recursive Table
                val schema = StructType(structFields)
                attributes = structFields.map(sf => AttributeReference(sf.name, sf.dataType, nullable = false)())
                val rrdd = rasqlContext.sparkContext.emptyRDD[InternalRow]
                val rdf = rasqlContext.internalCreateDataFrame(t, rrdd, schema).toDF(attributes.map(_.name):_*)

                rasqlContext.setRecursiveRDD(t, rrdd)
                rasqlContext.registerDataFrameAsTable(rdf, t)
                t
        }

    /**
     * Computing Recursive Select - the base relation joins with the recursive
     */
    lazy val recursive_select: Parser[Project] =
        SELECT ~> repsep(projection, ",") ~
            (FROM   ~> recursive_relations) ~
            (WHERE  ~> expression).? ^^ {
            case p ~ rr ~ c =>
                val recRelation = rr._1
                val rel = rr._2

                val cachedRelation = CacheHint(rel)
                val joined  = Join(cachedRelation, recRelation, Inner, c)

                val uaAggregationKey = UnresolvedAlias(Alias(p.head, aggregationKey.name)())
                val toProject =
                    if (partialPremF != null) {
                        val last = UnresolvedAlias(Alias(p.last, premKey.name)())
                        val restKeys = p.drop(1).dropRight(1)
                            .zip(restAttributes).map{ case (e, ua) => UnresolvedAlias(Alias(e, ua.name)())}
                        uaAggregationKey +: restKeys :+ last
                    }
                    else {
                        val restKeys = p.drop(1).zip(restAttributes).map{ case (e, ua) => UnresolvedAlias(Alias(e, ua.name)())}
                        uaAggregationKey +: restKeys
                    }

                Project(toProject, joined)
        }

    /**
     * Plain select query
     */
    protected lazy val simple_select: Parser[Project] =
        SELECT ~> rep1sep(projection, ",") ~
            (FROM   ~> relations).? ~
            (WHERE  ~> expression).? ^^ {
            case p ~ r ~ f =>
                val base = r.getOrElse(OneRowRelation)
                val withFilter = f.map(Filter(_, base)).getOrElse(base)
                val withProject = Project(p.zip(attributes).map{case (e, ua) => UnresolvedAlias(Alias(e, ua.name)())}, withFilter)
                withProject
        }

    /**
     * Parsing PreM Function declaration
     */
    lazy val premFunction: Parser[AggregateExpression] =
       ( (MCOUNT| MMAX | MSUM | MMIN ) ~ (AS ~> expression)) ^^ { case premF ~ e =>
           partialPremF = lexical.normalizeKeyword(premF) match {
                case "mcount" => AggregateExpression(MCount(e), mode = Partial, isDistinct = true)
                case "msum" => AggregateExpression(MSum(e), mode = Partial, isDistinct = true)
                case "mmin" => AggregateExpression(MMin(e), mode = Partial, isDistinct = true)
                case "mmax" => AggregateExpression(MMax(e), mode = Partial, isDistinct = true)
                case _ => throw new AnalysisException(s"invalid expression $premF")
            }
           finalPremF = lexical.normalizeKeyword(premF) match {
               case "mcount" => AggregateExpression(MCount(e), mode = Complete, isDistinct = true)
               case "msum" => AggregateExpression(MSum(e), mode = Complete, isDistinct = true)
               case "mmin" => AggregateExpression(MMin(e), mode = Complete, isDistinct = true)
               case "mmax" => AggregateExpression(MMax(e), mode = Complete, isDistinct = true)
               case _ => throw new AnalysisException(s"invalid expression $premF")
           }
           functionName = lexical.normalizeKeyword(premF)
           premKey = UnresolvedAttribute(e.asInstanceOf[UnresolvedAttribute].name)
           rasqlContext.setPremF(partialPremF.aggregateFunction)
           partialPremF
        }

    def getFunction(e: Expression, f: String): AggregateFunction ={
        f match {
            case "mcount" => MCount(e)
            case "msum" => MSum(e)
            case "mmin" => MMin(e)
            case "mmax" => MMax(e)
        }
    }


    /**
     * Joining the Recursive Relation with the other relations
     * The Recursive Relation is always the first.
     */
    lazy val recursive_relations: Parser[(Subquery, LogicalPlan)] =
        tableIdentifier ~ ("," ~> relation)^^ {
            case rr ~ rel =>
                val recursiveRelation = Subquery(rr.unquotedString, RecursiveRelation(rr, attributes))
                (recursiveRelation, rel)
        }

    // Regular SQL Parsers follow

    lazy val projection: Parser[Expression] = expression

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
