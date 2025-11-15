import React from 'react';
import { DragDropContext, Droppable, Draggable, DropResult } from 'react-beautiful-dnd';
import { ExpressionItem } from '../types';
import './Canvas.css';

interface CanvasProps {
  items: ExpressionItem[];
  setItems: React.Dispatch<React.SetStateAction<ExpressionItem[]>>;
}

const Canvas: React.FC<CanvasProps> = ({ items, setItems }) => {
  const onDragEnd = (result: DropResult) => {
    const { source, destination } = result;
    if (!destination) return;
    const reorderedItems = Array.from(items);
    const [removed] = reorderedItems.splice(source.index, 1);
    reorderedItems.splice(destination.index, 0, removed);
    setItems(reorderedItems);
  };
  const handleRemoveItem = (index: number) => {
    setItems(items.filter((_, i) => i !== index));
  };
  return (
    <DragDropContext onDragEnd={onDragEnd}>
      <Droppable droppableId="canvas-droppable" direction="horizontal">
        {(provided) => (
          <div className="canvas" {...provided.droppableProps} ref={provided.innerRef}>
            {items.length === 0 ? (
              <p className="canvas-placeholder">팔레트에서 아이템을 클릭하세요</p>
            ) : (
              items.map((item, index) => (
                <Draggable key={`${item.id}-${index}`} draggableId={`${item.id}-${index}`} index={index}>
                  {(provided) => (
                    <div ref={provided.innerRef} {...provided.draggableProps} {...provided.dragHandleProps}
                         className={`canvas-item item-type-${item.type}`}>
                      {item.label}
                      <button onClick={() => handleRemoveItem(index)} className="remove-item-btn">×</button>
                    </div>
                  )}
                </Draggable>
              ))
            )}
            {provided.placeholder}
          </div>
        )}
      </Droppable>
    </DragDropContext>
  );
};

export default Canvas;