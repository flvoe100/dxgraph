/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Property implementation for testing.
 * @author Philipp Rehs
 */
public class LabelProperty extends Property<LabelProperty> {

    static {
        PropertyManager.registerPropertyClass(LabelProperty.class);
    }

    private String m_value;

    /**
     * Constructor
     */
    public LabelProperty() {
    }

    /**
     * Get the value.
     * @return Value.
     */
    public String getValue() {
        return m_value;
    }

    /**
     * Set the value.
     * @param val
     *            Value.
     */
    public void setValue(final String val) {
        m_value = val;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_value);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_value = p_importer.readString(m_value);
    }

    @Override
    public int sizeofObject() {
        return m_value.length();
    }
}
