package io.github.riverbench.ci_worker
package util.rdf

import com.apicatalog.jsonld.*
import com.apicatalog.jsonld.api.FramingApi
import com.apicatalog.jsonld.document.{JsonDocument, RdfDocument}
import com.apicatalog.jsonld.lang.Keywords
import com.apicatalog.jsonld.processor.FromRdfProcessor
import jakarta.json.Json
import jakarta.json.stream.JsonGenerator
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.rdf.model.{Model, ModelFactory, Resource}
import org.apache.jena.riot.system.JenaTitanium
import org.apache.jena.sparql.core.DatasetGraphFactory
import org.apache.jena.sparql.vocabulary.FOAF
import org.apache.jena.vocabulary.*

import java.io.FileOutputStream
import java.nio.file.Path
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object JsonLdEmbedding:
  private val emptyJsonDoc = JsonDocument.of(Json.createObjectBuilder.build)
  private val writerConfig = util.Map.of(JsonGenerator.PRETTY_PRINTING, true)
  private val writerFactory = Json.createWriterFactory(writerConfig)
  
  private val urlProps = Set(SchemaDO.url, SchemaDO.sameAs, SchemaDO.contentUrl, SchemaDO.license)

  private val typeMap = Map(
    DCAT.Dataset -> SchemaDO.Dataset,
    DCAT.Catalog -> SchemaDO.DataCatalog,
    DCAT.Distribution -> SchemaDO.DataDownload,
  )

  private val propMap = Map(
    DCTerms.description -> SchemaDO.description,
    DCTerms.title -> SchemaDO.alternateName,
    DCTerms.identifier -> SchemaDO.identifier,
    DCTerms.creator -> SchemaDO.creator,
    // "To uniquely identify individuals, use ORCID ID as the value of the sameAs property of the Person type."
    FOAF.homepage -> SchemaDO.sameAs,
    FOAF.name -> SchemaDO.name,
    FOAF.nick -> SchemaDO.alternateName,
    RDFS.comment -> SchemaDO.description,
    DCTerms.license -> SchemaDO.license,
    RdfUtil.dcatVersion -> SchemaDO.version,
    DCAT.landingPage -> SchemaDO.url,
    DCAT.distribution -> SchemaDO.distribution,
    DCAT.downloadURL -> SchemaDO.contentUrl,
    DCAT.mediaType -> SchemaDO.encodingFormat,
    RdfUtil.dcatInCatalog -> SchemaDO.includedInDataCatalog,
    DCTerms.issued -> SchemaDO.datePublished,
    DCTerms.modified -> SchemaDO.dateModified,
    DCAT.theme -> SchemaDO.about,
    DCAT.byteSize -> SchemaDO.contentSize,
    DCAT.dataset -> SchemaDO.dataset,
    RdfUtil.dcatInSeries -> SchemaDO.isPartOf,
    RdfUtil.dcatSeriesMember -> SchemaDO.hasPart,
  )

  /**
   * Saves a JSON-LD representation of metadata for embedding in HTML.
   *
   * The method will remove any cycles in the RDF graph starting from the given subject,
   * and frame the JSON-LD output to only include the subject and its descendants.
   *
   * @param m RDF model
   * @param mdFilePath file name of the original markdown file
   */
  def saveEmbed(m: Model, subject: Resource, mdFilePath: Path): Unit =
    val mCopy = translateDcatToSchema(m, subject)
    // val mCopy = ModelFactory.createDefaultModel().add(m)

    removeCycles(mCopy, subject)
    val fName = mdFilePath.getFileName.toString.replace(".md", "_embed.jsonld")
    val outPath = mdFilePath.getParent.resolve(fName)
    val dsg = DatasetGraphFactory.create(mCopy.getGraph)
    val doc = RdfDocument.of(JenaTitanium.convert(dsg))
    val options = JsonLdOptions()
    options.setEmbed(JsonLdEmbed.ALWAYS)

    // Convert the RDF to JSON
    val array = FromRdfProcessor.fromRdf(doc, options)
    val context = Json.createObjectBuilder
      .add("@vocab", "https://schema.org/")
      .build
    val jsonBare = Json.createObjectBuilder
      .add(Keywords.CONTEXT, context)
      .add(Keywords.GRAPH, array)
      .build
    val contextDoc = JsonDocument.of(context)

    // Compaction & framing
    val jsonFramed = FramingApi(JsonDocument.of(jsonBare), emptyJsonDoc)
      .options(options).get
    val jsonCompacted = JsonLd.compact(JsonDocument.of(jsonFramed), contextDoc).get

    // Finally, extract only the subject tree
    val subjectTree = jsonCompacted.getJsonArray("@graph").asScala
      .find(obj => {
        obj.asJsonObject().containsKey("@id") &&
        obj.asJsonObject().getString("@id") == subject.getURI
      })
      .get
    val finalJson = Json.createObjectBuilder
      .add("@context", jsonCompacted.getJsonObject("@context"))
      .addAll(Json.createObjectBuilder(subjectTree.asJsonObject()))
      .build

    // and save it
    val os = new FileOutputStream(outPath.toFile)
    val writer = writerFactory.createWriter(os)
    writer.writeObject(finalJson)
    os.close()
    println("Saved JSON-LD embed to " + outPath)


  private def translateDcatToSchema(input: Model, subject: Resource): Model =
    val output = ModelFactory.createDefaultModel()
    val typeStmts = input.listStatements(null, RDF.`type`, null)
    var typeName = "resource"
    while typeStmts.hasNext do
      val stmt = typeStmts.nextStatement()
      val subj = stmt.getSubject
      val obj = stmt.getObject.asResource()
      val newType = typeMap.getOrElse(obj, obj)
      output.add(subj, RDF.`type`, newType)
      if newType == SchemaDO.Dataset then
        output.add(subj, SchemaDO.isAccessibleForFree, "true", XSDDatatype.XSDboolean)
        typeName = "dataset"
      else if newType == SchemaDO.DataCatalog then
        typeName = "catalog"
      else if obj == RdfUtil.Profile then
        typeName = "profile"
      else if obj == RdfUtil.Task then
        typeName = "task"
      else if obj == RdfUtil.Category then
        typeName = "category"

    val propStmts = input.listStatements(null, null, null)
    while propStmts.hasNext do
      val stmt = propStmts.nextStatement()
      val subj = stmt.getSubject
      val pred = stmt.getPredicate
      val obj = stmt.getObject
      if pred != RDF.`type` then
        val newPred = propMap.get(pred)
        if newPred.isDefined then
          if obj.isResource && urlProps.contains(newPred.get) then
            output.add(subj, newPred.get, obj.asResource().getURI)
          else output.add(subj, newPred.get, obj)
      if pred == FOAF.name then
        output.add(subj, RDF.`type`, SchemaDO.Person)

    val id = input.listObjectsOfProperty(subject, DCTerms.identifier).asScala
      .nextOption().map(_.asLiteral().getString)
    if id.isDefined then
      output.add(subject, SchemaDO.name, s"RiverBench $typeName: $id")
    else output.add(subject, SchemaDO.name, s"RiverBench catalog")

    // Publisher
    val publisher = output.createResource("https://w3id.org/riverbench")
    output.add(subject, SchemaDO.publisher, publisher)
    output.add(publisher, RDF.`type`, SchemaDO.Organization)
    output.add(publisher, SchemaDO.name, "RiverBench")
    output.add(publisher, SchemaDO.description, 
      "RiverBench is an open, community-driven RDF streaming benchmark suite. " +
        "It includes a varied collection of datasets and tasks, representing many real-life use cases."
    )
    output.add(publisher, SchemaDO.url, "https://w3id.org/riverbench")
    output.add(publisher, SchemaDO.logo, "https://w3id.org/riverbench/assets/riverbench_vector_logo.png")
    output


  private def removeCycles(m: Model, subject: Resource, visited: mutable.Set[Resource] = mutable.Set.empty): Unit =
    visited += subject
    val objects = subject.listProperties().asScala.toSeq
      .filter(_.getObject.isResource)
      .map(_.getObject.asResource)
      .filterNot(visited.contains)

    if objects.nonEmpty then
      objects
        .flatMap(m.listStatements(_, null, subject).asScala)
        .foreach(st => {
          m.remove(st)
          // debug
          // println(s"Removed cycle: ${st.getSubject} -> ${st.getPredicate} -> ${st.getObject}")
        })
      objects.foreach(removeCycles(m, _, visited))
