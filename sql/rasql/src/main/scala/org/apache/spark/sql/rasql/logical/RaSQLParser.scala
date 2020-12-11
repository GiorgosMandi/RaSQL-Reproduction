package org.apache.spark.sql.rasql.logical

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Ascending, AttributeReference, BinaryExpression, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, Descending, Divide, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Like, Literal, Multiply, Not, Or, RLike, Remainder, SortDirection, SortOrder, Subtract, UnaryMinus, aggregate}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.catalyst.{AbstractSparkSQLParser, InternalRow, TableIdentifier}
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SQLContext}

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
    var functionName: String = _
    val attributesAliases: List[UnresolvedAttribute] = List() // the first is always the aggregation key and the last is PreM functions target
    var preMKey: UnresolvedAttribute = _
    var aggregationKey: UnresolvedAttribute = _
    var restAttributes: Seq[UnresolvedAttribute] = _

    lazy val rasql: Parser[LogicalPlan] =
        (recursive_cte) ~
            (AS ~> ( "(" ~> simple_select <~ ")")) ~
            (UNION ~> ("(" ~> recursive_select <~ ")")) ~
            (simple_select) ^^{
            case t ~ base ~ recursive ~ s =>

                rasqlContext.recursiveTable = t
                if(preMKey != null) {
                    // Form Monotonic Aggregation of recursive cte
                    // Form PreM of target attribute
                    val recPreMTarget = Seq(UnresolvedAttribute(preMKey.name))
                    val recPreM = UnresolvedFunction(functionName, recPreMTarget, isDistinct = true)
                    val recPreMAAliased = UnresolvedAlias(Alias(recPreM, preMKey.name)())
                    // Form aggregation Attributes
                    val recAggrKey = UnresolvedAttribute(aggregationKey.name)

                    val recAggregateArgs = Seq(UnresolvedAlias(Alias(recAggrKey, aggregationKey.name)()), recPreMAAliased)
                    val recursiveMA = MonotonicAggregatePartial(Seq(recAggrKey), recAggregateArgs, Subquery("RR", recursive))

                    // Form Monotonic Aggregation of base cte - This stays the same so to
                    // Form PreM of target attribute
                    val basePreMTarget = Seq(base.projectList.last)
                    val basePreM = UnresolvedFunction(functionName, basePreMTarget, isDistinct = true)
                    val basePreMAliased = UnresolvedAlias(Alias(basePreM, preMKey.name)())
                    // Form aggregation Attributes
                    val baseAggrKey = Seq(base.projectList.head)
                    val baseAggregateArgs = base.projectList.dropRight(1) :+ basePreMAliased
                    val baseMA = MonotonicAggregatePartial(baseAggrKey, baseAggregateArgs, base)

                    // In AggregateRecursion we apply the recursion
                    val ra = RecursiveAggregate(t, baseMA, recursiveMA)

                    // After Recursive Aggregation we apply again the PreM F
                    val allKeys = attributes.map(ar => UnresolvedAttribute(ar.name))
                    val lastKey = Seq(allKeys.last)
                    val preM = UnresolvedFunction(functionName, lastKey, isDistinct = true)
                    val preMAliased = UnresolvedAlias(Alias(preM, preMKey.name)())
                    val groupingKeys = allKeys.dropRight(1)
                    val raMA = MonotonicAggregateGlobal(groupingKeys, groupingKeys :+ preMAliased, Subquery(t + "_RA", ra))

                    // After initializing the Recursive CTE we apply the select query
                    val w = With(s, Map(t -> Subquery(t, raMA)))
                    w
                }
                else {
                    val recAggrKey = UnresolvedAttribute(aggregationKey.name)
                    val recAggregateArgs = Seq(UnresolvedAlias(Alias(recAggrKey, aggregationKey.name)()))
                    val recAggregate = Aggregate(Seq(recAggrKey), recAggregateArgs, Subquery("RR", recursive))

                    val ra = RecursiveAggregate(t, base, recAggregate)
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
                // Store info for PreM Function and other
                // p will always have size=2 consisting of the aggregate key and the PreMFunction(key)
                val structFields = p.last match {
                    case pf: RecursiveAggregateExpr =>
                        functionName = pf.getFunctionName
                        preMKey = pf.getTargetAttributeAlias
                        aggregationKey = p.head.asInstanceOf[UnresolvedAttribute]
                        restAttributes = p.drop(1).dropRight(1).asInstanceOf[Seq[UnresolvedAttribute]]

                        p.dropRight(1)
                            .map(ua => StructField(ua.asInstanceOf[UnresolvedAttribute].name, IntegerType, nullable = false)) :+ StructField(preMKey.name, IntegerType, nullable = false)
                    case _ =>
                        aggregationKey = p.head.asInstanceOf[UnresolvedAttribute]
                        restAttributes = p.drop(1).asInstanceOf[Seq[UnresolvedAttribute]]
                        p.map(ua => StructField(ua.asInstanceOf[UnresolvedAttribute].name, IntegerType, nullable = false))
                }

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

                val bo = c.asInstanceOf[Some[BinaryExpression]].get
                val cachedRelation = CacheHint(rel)
                val joined  = Join(cachedRelation, recRelation, Inner, c)

                val uaAggregationKey = UnresolvedAlias(Alias(p.head, aggregationKey.name)())
                val restKeys = p.dropRight(1).drop(1).zipWithIndex.map{ case (e: Expression, i) => UnresolvedAlias(Alias(e, restAttributes(i).name)())}
                val toProject: Seq[UnresolvedAlias] = if (preMKey != null)
                      uaAggregationKey +: restKeys :+ UnresolvedAlias(Alias(p.last, preMKey.name)())
                else uaAggregationKey +: restKeys
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
                val withProject = Project(p.map(UnresolvedAlias), withFilter)
                withProject
        }

    /**
     * Parsing PreM Function declaration
     */
    lazy val premFunction: Parser[RecursiveAggregateExpr] =
       ( (MCOUNT| MMAX | MSUM | MMIN ) ~ (AS ~> expression)) ^^ { case premF ~ e =>
            lexical.normalizeKeyword(premF) match {
                case "mcount" => RecursiveAggregateExpr(MCount(e), mode = Complete, fName = "mcount", attrName = e)
                case "msum" => RecursiveAggregateExpr(MSum(e), mode = Complete, fName = "msum", attrName = e)
                case "mmin" => RecursiveAggregateExpr(MMin(e), mode = Complete, fName = "mmin", attrName = e)
                case "mmax" => RecursiveAggregateExpr(MMax(e), mode = Complete, fName = "mmax", attrName = e)
                case _ => throw new AnalysisException(s"invalid expression $premF")
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
