import React from 'react';
import { ExpressionItem } from '../types';
import './Palette.css';

interface PaletteProps {
  onItemAdd: (item: ExpressionItem) => void;
}

const PALETTE_ITEMS: ExpressionItem[] = [
  { id: 'close', type: 'data', label: 'close' }, { id: 'open', type: 'data', label: 'open' },
  { id: 'high', type: 'data', label: 'high' }, { id: 'low', type: 'data', label: 'low' },
  { id: 'volume', type: 'data', label: 'volume' }, { id: 'amount', type: 'data', label: 'amount' },
  { id: '>', type: 'operator', label: '>' }, { id: '>=', type: 'operator', label: '>=' },
  { id: '<', type: 'operator', label: '<' }, { id: '<=', type: 'operator', label: '<=' },
  { id: '==', type: 'operator', label: '==' }, { id: '!=', type: 'operator', label: '!=' },
  { id: '+', type: 'operator', label: '+' }, { id: '-', type: 'operator', label: '-' },
  { id: '*', type: 'operator', label: '*' }, { id: '/', type: 'operator', label: '/' },
  { id: 'AND', type: 'operator', label: 'AND' }, { id: 'OR', type: 'operator', label: 'OR' },
  { id: '(', type: 'operator', label: '(' }, { id: ')', type: 'operator', label: ')' },
  { id: '100', type: 'number', label: '100' }, { id: '0', type: 'number', label: '0' },
  { id: 'trix(12)', type: 'indicator', label: 'trix(12)' }, { id: 'ma(20)', type: 'indicator', label: 'ma(20)' },
  { id: 'trix12', type: 'variable', label: 'trix12' },
  { id: 'shift(1)', type: 'function', label: 'shift(1)' }, { id: 'cross_up', type: 'function', label: 'cross_up' },
];

const Palette: React.FC<PaletteProps> = ({ onItemAdd }) => {
  const handleItemClick = (item: ExpressionItem) => onItemAdd(item);
  return (
    <div className="palette">
      <h3>팔레트 (도구 모음)</h3>
      <div className="palette-items">
        {PALETTE_ITEMS.map(item => (
          <button key={item.id} onClick={() => handleItemClick(item)} className={`palette-item item-type-${item.type}`}>
            {item.label}
          </button>
        ))}
      </div>
    </div>
  );
};

export default Palette;