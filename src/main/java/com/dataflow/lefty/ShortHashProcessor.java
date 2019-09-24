package com.dataflow.lefty;

import java.util.List;
import java.util.Map;

/**
 * Created by Owen Rees-Hayward on 03/11/2016.
 */
public final class ShortHashProcessor implements Processor<HashShort,Long> {

    private long comparisonHash;
    private int maxDistance;

    @Override
    public void setContext(final Map<String, String> ctx) {
        this.comparisonHash = Long.valueOf(ctx.get("comparisonHash"));
        this.maxDistance = Integer.valueOf(ctx.get("maxDistance"));
    }

    @Override
    public final void processDatum(final List<Long> matches, final HashShort fly) {
        final long hash = fly.getValue();

        if (Long.bitCount(hash ^ comparisonHash) < maxDistance) {
            matches.add(hash);
        }
    }
}
