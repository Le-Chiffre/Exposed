package kotlin.generate

import com.google.auto.service.AutoService
import java.io.BufferedWriter

import javax.annotation.processing.*
import javax.lang.model.SourceVersion
import javax.lang.model.element.Element
import javax.lang.model.element.ElementKind
import javax.lang.model.element.TypeElement
import javax.lang.model.util.Elements
import javax.lang.model.util.Types
import javax.tools.Diagnostic
import java.util.*
import javax.lang.model.element.PackageElement

@AutoService(Processor::class)
public class TypeGenerator : AbstractProcessor() {
    private var types: Types? = null
    private var elements: Elements? = null
    private var filer: Filer? = null
    private var messager: Messager? = null
    private val valueClasses = LinkedHashMap<String, Class<Any>>()

    @Synchronized override fun init(environment: ProcessingEnvironment) {
        super.init(environment)
        types = environment.typeUtils
        elements = environment.elementUtils
        filer = environment.filer
        messager = environment.messager
    }

    override fun getSupportedAnnotationTypes(): Set<String> {
        val annotations = LinkedHashSet<String>()
        annotations.add(Value::class.qualifiedName!!)
        return annotations
    }

    override fun getSupportedSourceVersion(): SourceVersion {
        return SourceVersion.latestSupported()
    }

    override fun process(annotations: Set<TypeElement>, environment: RoundEnvironment): Boolean {
        for (e in environment.getElementsAnnotatedWith(Value::class.java)) {
            // Make sure that this is a class and a subtype of BasdeLookupTable.
            if (e.kind != ElementKind.CLASS) {
                error(e, "Only classes can be annotated with Value")
                continue
            }

            val type = e as TypeElement
            if (!types!!.isSubtype(type.asType(), elements!!.getTypeElement("kotlin.sql.BaseLookupTable").asType())) {
                error(e, "Only subclasses of BaseLookupTable can be annotated with Value")
                continue
            }

            // Get the generated type name.
            val annotation = e.getAnnotation(Value::class.java)
            val name = if(annotation.value == "") "${e.simpleName}Value" else annotation.value

            processTable(name, e)
        }

        return true
    }

    private fun processTable(name: String, e: TypeElement) {
        message("Generating class $name.")

        val pack = e.enclosingElement as PackageElement
        val file = filer!!.createSourceFile("${pack.qualifiedName}.name")

        with(BufferedWriter(file.openWriter())) {
            // Class header.
            append("package ${pack.qualifiedName};")
            append("class $name {")
            newLine()

            // Class members.
            for(v in e.enclosedElements) {

            }

            flush()
        }
    }

    private fun error(e: Element, msg: String) = messager!!.printMessage(Diagnostic.Kind.ERROR, msg, e)
    private fun message(msg: String) = messager!!.printMessage(Diagnostic.Kind.NOTE, msg)
}