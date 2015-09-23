package storm.starter.trident.project.functions;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
/**
 * Function that just emits the lowercased text.
 *@author: Pratyush Oak
 */
@SuppressWarnings("serial")
public class ToLowerCaseFunction extends BaseFunction {
@Override
public void execute(TridentTuple tuple, TridentCollector collector) { collector.emit(new Values(tuple.getString(0).toLowerCase()));
} }
