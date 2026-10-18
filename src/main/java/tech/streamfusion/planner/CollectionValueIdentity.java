package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/**
 * Restricts representation-dependent values to boundaries verified to carry their Java semantics.
 */
final class CollectionValueIdentity {
  private CollectionValueIdentity() {}

  static boolean sensitive(RelDataType type) {
    return JsonStringIdentity.containsCharacter(type) || requiresCopySemantics(type);
  }

  private static boolean requiresCopySemantics(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ROW
        || type.getSqlTypeName() == SqlTypeName.STRUCTURED
        || type.getSqlTypeName() == SqlTypeName.MAP
        || type.getSqlTypeName() == SqlTypeName.ARRAY && !type.getComponentType().isNullable()
        || type.getComponentType() != null && requiresCopySemantics(type.getComponentType())
        || type.getKeyType() != null && requiresCopySemantics(type.getKeyType())
        || type.getValueType() != null && requiresCopySemantics(type.getValueType());
  }

  static boolean fixedBinary(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.BINARY
        || type.getComponentType() != null && fixedBinary(type.getComponentType())
        || type.getKeyType() != null && fixedBinary(type.getKeyType())
        || type.getValueType() != null && fixedBinary(type.getValueType())
        || type.isStruct()
            && type.getFieldList().stream().anyMatch(field -> fixedBinary(field.getType()));
  }

  static boolean requiresGeneratedEvaluation(RexProgram program) {
    if (!RexExpression.usesCollectionFunctions(program)) return false;
    return program.getExprList().stream()
        .anyMatch(CollectionValueIdentity::consumerNeedsGeneration);
  }

  private static boolean consumerNeedsGeneration(RexNode node) {
    if (!(node instanceof RexCall)) return false;
    var call = (RexCall) node;
    return !call.getOperator().getName().equals("CARDINALITY")
            && call.getOperands().stream().anyMatch(arg -> sensitive(arg.getType()))
        || call.getOperands().stream().anyMatch(CollectionValueIdentity::consumerNeedsGeneration);
  }

  static boolean unverifiedInput(Calc calc) {
    var program = calc.getProgram();
    return RexExpression.usesCollectionFunctions(program)
        && (program.getProjectList().stream()
                .anyMatch(ref -> readsSensitiveInput(program.expandLocalRef(ref)))
            || program.getCondition() != null
                && readsSensitiveInput(program.expandLocalRef(program.getCondition())))
        && unverifiedColumnarProducer(calc.getInput());
  }

  private static boolean readsSensitiveInput(RexNode node) {
    if (node instanceof RexInputRef) return sensitive(node.getType());
    if (node instanceof RexFieldAccess)
      return readsSensitiveInput(((RexFieldAccess) node).getReferenceExpr());
    if (!(node instanceof RexCall)) return false;
    var call = (RexCall) node;
    return !call.getOperator().getName().equals("CARDINALITY")
        && call.getOperands().stream().anyMatch(CollectionValueIdentity::readsSensitiveInput);
  }

  private static boolean unverifiedColumnarProducer(RelNode node) {
    if (!(node instanceof ColumnarOutput)) return false;
    if (node instanceof StreamPhysicalRowDataToArrow)
      return !((StreamPhysicalRowDataToArrow) node).preservesCollections();
    if (node instanceof StreamPhysicalNativeCalc
        && !((StreamPhysicalNativeCalc) node).preservesCollections()) return true;
    if (node instanceof StreamPhysicalNativeCalc
        || node instanceof StreamPhysicalNativeMiniBatchAssigner)
      return node.getInputs().stream()
          .anyMatch(CollectionValueIdentity::unverifiedColumnarProducer);
    return true;
  }

  static boolean crossesBoundary(RelNode root, boolean finalOutput) {
    if (!NativeConfig.collectionExpressionsEnabled()) return false;
    return crossesBoundary(root, null, finalOutput);
  }

  private static boolean crossesBoundary(RelNode node, RelNode parent, boolean finalOutput) {
    if (node instanceof Calc
        && !(parent == null && finalOutput)
        && !(parent instanceof StreamPhysicalSink)) {
      var program = ((Calc) node).getProgram();
      if (RexExpression.usesCollectionFunctions(program)
          && program.getProjectList().stream().anyMatch(ref -> sensitive(ref.getType())))
        return true;
    }
    return node.getInputs().stream().anyMatch(input -> crossesBoundary(input, node, false));
  }
}
