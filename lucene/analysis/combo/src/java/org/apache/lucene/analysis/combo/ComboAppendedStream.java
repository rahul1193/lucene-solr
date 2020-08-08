package org.apache.lucene.analysis.combo;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.Attribute;

import java.io.IOException;
import java.util.Iterator;

/**
 * A TokenStream combining the output of multiple sub-TokenStreams.
 * This class copies the attributes from the last sub-TokenStream that
 * was read from. If attributes are not uniform between sub-TokenStreams,
 * extraneous attributes will stay untouched.
 */
public class ComboAppendedStream extends TokenStream {

    private final PositionedTokenStream[] tokenStreams;
    private final int gap;

    private int currentStream = 0;
    private int lastPosition = 0;
    private int curStreamPosition = 0;
    private int totalStreamPosition = 0;

    public ComboAppendedStream(int gap, TokenStream... tokenStreams) {
        // Load the TokenStreams, track their position, and register their attributes
        this.tokenStreams = new PositionedTokenStream[tokenStreams.length];
        for (int i = 0; i < tokenStreams.length; i++) {
            if (tokenStreams[i] == null) {
                continue;
            }
            this.tokenStreams[i] = new PositionedTokenStream(tokenStreams[i]);
            // Add each and every token seen in the current sub AttributeSource
            Iterator<Class<? extends Attribute>> iterator = this.tokenStreams[i].getAttributeClassesIterator();
            while (iterator.hasNext()) {
                addAttribute(iterator.next());
            }
        }
        this.gap = gap < 0 ? 0 : gap;
    }

    /*
     * TokenStream multiplexed methods
     */
    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();
        if (currentStream >= tokenStreams.length) {
            return false;
        }

        PositionedTokenStream tokenStream = tokenStreams[currentStream];
        if (tokenStream == null || !tokenStream.incrementToken()) {
            currentStream++;
            if (curStreamPosition > 0) {
                totalStreamPosition = totalStreamPosition + curStreamPosition + gap;
            }
            return incrementToken();
        }

        curStreamPosition = tokenStream.getPosition();
        int expectedPosition = totalStreamPosition + curStreamPosition;

        // Copy the current token attributes from the sub-TokenStream to our AttributeSource
        restoreState(tokenStream.captureState());

        // Override the PositionIncrementAttribute
        int incr = Math.max(0, expectedPosition - lastPosition);
        this.getAttribute(PositionIncrementAttribute.class).setPositionIncrement(incr);
        lastPosition += incr;
        return true;
    }

    @Override
    public void end() throws IOException {
        super.end();
        this.lastPosition = 0;
        this.currentStream = 0;
        this.curStreamPosition = 0;
        this.totalStreamPosition = 0;
        // Apply on each sub-TokenStream
        for (PositionedTokenStream pts : tokenStreams) {
            if (pts == null) continue;
            pts.end();
        }
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        clearAttributes();
        this.lastPosition = 0;
        this.currentStream = 0;
        this.curStreamPosition = 0;
        this.totalStreamPosition = 0;
        // Apply on each sub-TokenStream
        for (PositionedTokenStream pts : tokenStreams) {
            if (pts == null) continue;
            pts.reset();
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.lastPosition = 0;
        this.currentStream = 0;
        this.curStreamPosition = 0;
        this.totalStreamPosition = 0;
        // Apply on each sub-TokenStream
        for (PositionedTokenStream pts : tokenStreams) {
            if (pts == null) continue;
            pts.close();
        }
    }

}
